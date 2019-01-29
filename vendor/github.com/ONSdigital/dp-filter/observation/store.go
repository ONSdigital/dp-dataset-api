package observation

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

//go:generate moq -out observationtest/db_pool.go -pkg observationtest . DBPool

// Store represents storage for observation data.
type Store struct {
	pool DBPool
}

// DBPool provides a pool of database connections
type DBPool interface {
	OpenPool() (bolt.Conn, error)
}

// NewStore returns a new store instace using the given DB connection.
func NewStore(pool DBPool) *Store {
	return &Store{
		pool: pool,
	}
}

// GetCSVRows returns a reader allowing individual CSV rows to be read. Rows returned
// can be limited, to stop this pass in nil. If filter.DimensionFilters is nil, empty or contains only empty values then
// a CSVRowReader for the entire dataset will be returned.
func (store *Store) GetCSVRows(filter *Filter, limit *int) (CSVRowReader, error) {

	headerRowQuery := fmt.Sprintf("MATCH (i:`_%s_Instance`) RETURN i.header as row", filter.InstanceID)

	unionQuery := headerRowQuery + " UNION ALL " + createObservationQuery(filter)

	if limit != nil {
		limitAsString := strconv.Itoa(*limit)
		unionQuery += " LIMIT " + limitAsString
	}

	log.Info("neo4j query", log.Data{
		"filterID":   filter.FilterID,
		"instanceID": filter.InstanceID,
		"query":      unionQuery,
	})
	conn, err := store.pool.OpenPool()
	if err != nil {
		return nil, err
	}

	rows, err := conn.QueryNeo(unionQuery, nil)
	if err != nil {
		// Before returning the error "close" the open connection to release it back into the pool.
		conn.Close()
		return nil, err
	}
	// The connection can only be closed once the results have been read, so the row reader is responsible for
	// releasing the connection back into the pool
	return NewBoltRowReader(rows, conn), nil
}

func createObservationQuery(filter *Filter) string {
	if filter.IsEmpty() {
		// if no dimension filter are specified than match all observations
		log.Info("no dimension filters supplied, generating entire dataset query", log.Data{
			"filterID":   filter.FilterID,
			"instanceID": filter.InstanceID,
		})
		return fmt.Sprintf("MATCH(o: `_%s_observation`) return o.value as row", filter.InstanceID)
	}

	matchDimensions := "MATCH "
	where := " WHERE "

	count := 0
	for _, dimension := range filter.DimensionFilters {
		// If the dimension options is empty then don't bother specifying in the query as this will exclude all matches.
		if len(dimension.Options) > 0 {
			if count > 0 {
				matchDimensions += ", "
				where += " AND "
			}

			matchDimensions += fmt.Sprintf("(o)-[:isValueOf]->(`%s`:`_%s_%s`)", dimension.Name, filter.InstanceID, dimension.Name)
			where += createOptionList(dimension.Name, dimension.Options)
			count++
		}
	}

	return matchDimensions + where + " RETURN o.value AS row"
}

func createOptionList(name string, opts []string) string {
	var q []string

	for _, o := range opts {
		q = append(q, fmt.Sprintf("`%s`.value='%s'", name, o))
	}

	return fmt.Sprintf("(%s)", strings.Join(q, " OR "))
}
