package pagination

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strconv"

	"github.com/ONSdigital/log.go/v2/log"
)

// PaginatedHandler is a func type for an endpoint that returns a list of values that we want to paginate
type PaginatedHandler func(w http.ResponseWriter, r *http.Request, limit int, offset int) (list interface{}, totalCount int, err error)

type page struct {
	Items      interface{} `json:"items"`
	Count      int         `json:"count"`
	Offset     int         `json:"offset"`
	Limit      int         `json:"limit"`
	TotalCount int         `json:"total_count"`
}

type Paginator struct {
	DefaultLimit    int
	DefaultOffset   int
	DefaultMaxLimit int
}

func NewPaginator(defaultLimit, defaultOffset, defaultMaxLimit int) *Paginator {
	return &Paginator{
		DefaultLimit:    defaultLimit,
		DefaultOffset:   defaultOffset,
		DefaultMaxLimit: defaultMaxLimit,
	}
}

func (p *Paginator) getPaginationParameters(r *http.Request) (offset int, limit int, err error) {

	logData := log.Data{}
	offsetParameter := r.URL.Query().Get("offset")
	limitParameter := r.URL.Query().Get("limit")

	offset = p.DefaultOffset
	limit = p.DefaultLimit

	if offsetParameter != "" {
		logData["offset"] = offsetParameter
		offset, err = strconv.Atoi(offsetParameter)
		if err != nil || offset < 0 {
			err = errors.New("invalid query parameter")
			log.Error(r.Context(), "invalid query parameter: offset", err, logData)
			return 0, 0, err
		}
	}

	if limitParameter != "" {
		logData["limit"] = limitParameter
		limit, err = strconv.Atoi(limitParameter)
		if err != nil || limit < 0 {
			err = errors.New("invalid query parameter")
			log.Error(r.Context(), "invalid query parameter: limit", err, logData)
			return 0, 0, err
		}
	}

	if limit > p.DefaultMaxLimit {
		logData["max_limit"] = p.DefaultMaxLimit
		err = errors.New("invalid query parameter")
		log.Error(r.Context(), "limit is greater than the maximum allowed", err, logData)
		return 0, 0, err
	}
	return
}

func renderPage(list interface{}, offset int, limit int, totalCount int) page {

	return page{
		Items:      list,
		Count:      listLength(list),
		Offset:     offset,
		Limit:      limit,
		TotalCount: totalCount,
	}
}

func listLength(list interface{}) int {
	l := reflect.ValueOf(list)
	return l.Len()
}

// Paginate wraps a http endpoint to return a paginated list from the list returned by the provided function
func (p *Paginator) Paginate(paginatedHandler PaginatedHandler) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		offset, limit, err := p.getPaginationParameters(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		list, totalCount, err := paginatedHandler(w, r, limit, offset)
		if err != nil {
			return
		}

		renderedPage := renderPage(list, offset, limit, totalCount)

		returnPaginatedResults(w, r, renderedPage)
	}
}

func returnPaginatedResults(w http.ResponseWriter, r *http.Request, list page) {
	logData := log.Data{"path": r.URL.Path, "method": r.Method}

	b, err := json.Marshal(list)

	if err != nil {
		log.Error(r.Context(), "api endpoint failed to marshal resource into bytes", err, logData)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if _, err = w.Write(b); err != nil {
		log.Error(r.Context(), "api endpoint error writing response body", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Info(r.Context(), "api endpoint request successful", logData)
}
