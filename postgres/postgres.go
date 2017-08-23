package postgres

import (
	"database/sql"
	"github.com/ONSdigital/dp-dataset-api/models"
)

// Datastore represents a structure to hold SQL statements to be used to gather information or insert about filters and dimensions
type Datastore struct {
	datasetDB                       *sql.DB
	getDatasets                     *sql.Stmt
}

// NewDatastore manages a postgres datastore used to store and find information about filters and dimensions
func NewDatastore(datasetDB *sql.DB) (*Datastore, error) {
	getDatasets, err := prepare("SELECT datasetid, Datasets.name, nextrelease, Contacts.name, telephone, email FROM Datasets JOIN Contacts USING(contactid)", datasetDB)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	return &Datastore{datasetDB: datasetDB, getDatasets: getDatasets}, nil
}

func prepare(sql string, db *sql.DB) (*sql.Stmt, error) {
	statement, err := db.Prepare(sql)
	if err != nil {
		return statement, err
	}
	return statement, nil
}

func (ds *Datastore) GetAllDatasets() (*models.DatasetResults, error) {
	rows, err := ds.getDatasets.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	datasets := []models.Dataset{}
	for rows.Next() {
		var id, datasetName, nextRelease, telephone, name, email sql.NullString
		err = rows.Scan(&id, &datasetName, &nextRelease, &name, &telephone, &email)
		if err != nil {
			return nil, err
		}
		dataset := models.Dataset{ID: id.String, NextRelease: nextRelease.String, Name: datasetName.String,
			Contact: models.ContactDetails{Name: name.String, Email: email.String, Telephone: telephone.String}}
		datasets = append(datasets, dataset)
	}

	return &models.DatasetResults{Items: datasets}, nil
}
