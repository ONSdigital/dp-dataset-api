package pagination

import (
	"net/http"
	"reflect"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/utils"
	"github.com/ONSdigital/log.go/log"
)

func GetPaginationParameters(w http.ResponseWriter, r *http.Request) (offset int, limit int, err error) {

	cfg, _ := config.Get()

	logData := log.Data{}
	offsetParameter := r.URL.Query().Get("offset")
	limitParameter := r.URL.Query().Get("limit")

	offset = cfg.DefaultOffset
	limit = cfg.DefaultLimit

	if offsetParameter != "" {
		logData["offset"] = offsetParameter
		offset, err = utils.ValidatePositiveInt(offsetParameter)
		if err != nil {
			log.Event(r.Context(), "invalid query parameter: offset", log.ERROR, log.Error(err), logData)
			return 0, 0, err
		}
	}

	if limitParameter != "" {
		logData["limit"] = limitParameter
		limit, err = utils.ValidatePositiveInt(limitParameter)
		if err != nil {
			log.Event(r.Context(), "invalid query parameter: limit", log.ERROR, log.Error(err), logData)
			return 0, 0, err
		}
	}

	if limit > cfg.DefaultMaxLimit {
		logData["max_limit"] = cfg.DefaultMaxLimit
		err = errs.ErrInvalidQueryParameter
		log.Event(r.Context(), "limit is greater than the maximum allowed", log.ERROR, logData)
		return 0, 0, err
	}
	return
}

type Page struct {
	Items      interface{} `json:"items"`
	Count      int         `json:"count"`
	Offset     int         `json:"offset"`
	Limit      int         `json:"limit"`
	TotalCount int         `json:"total_count"`
}

func RenderPage(list interface{}, offset int, limit int, totalCount int) Page {

	return Page{
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
