package postgres

import "database/sql"

// Datastore represents a structure to hold SQL statements to be used to gather information or insert about filters and dimensions
type Datastore struct {
	datasetDB   *sql.DB
	getDatasets *sql.Stmt
}

// NewDatastore manages a postgres datastore used to store and find information about filters and dimensions
func NewDatastore(datasetDB *sql.DB) (Datastore, error) {
	getDatasets, err := prepare("SELECT * FROM Datasets(id, title, url, releaseDate,nextRelease,edition,version,contactId,instanceId)", datasetDB)
	if err != nil {
		return Datastore{datasetDB: datasetDB, getDatasets: getDatasets}, err
	}

	return Datastore{datasetDB: datasetDB, getDatasets: getDatasets}, nil
}

func prepare(sql string, db *sql.DB) (*sql.Stmt, error) {
	statement, err := db.Prepare(sql)
	if err != nil {
		return statement, err
	}
	return statement, nil
}
