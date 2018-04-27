package observation

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
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
	with := " WITH "
	match := " MATCH "

	for index, dimension := range filter.DimensionFilters {
		// If the dimension options is empty then don't bother specifying in the query as this will exclude all matches.
		if len(dimension.Options) > 0 {
			if index != 0 {
				matchDimensions += ", "
				where += " AND "
				with += ", "
				match += ", "
			}

			optionList := createOptionList(dimension.Options)
			matchDimensions += fmt.Sprintf("(%s:`_%s_%s`)", dimension.Name, filter.InstanceID, dimension.Name)
			where += fmt.Sprintf("%s.value IN [%s]", dimension.Name, optionList)
			with += dimension.Name
			match += fmt.Sprintf("(o:`_%s_observation`)-[:isValueOf]->(%s)", filter.InstanceID, dimension.Name)
		}
	}

	return matchDimensions + where + with + match + " RETURN o.value AS row"
}

func createOptionList(options []string) string {

	var buffer bytes.Buffer

	for index, option := range options {

		if index != 0 {
			buffer.WriteString(", ")
		}

		buffer.WriteString(fmt.Sprintf("'%s'", option))
	}

	return buffer.String()
}
