package datastore

import (
	"github.com/ONSdigital/dp-dataset-api/models"
	"gopkg.in/mgo.v2"
)

var _ Backend = &Mongo{}
var session *mgo.Session

// Mongo represents a simplistic MongoDB configuration.
type Mongo struct {
	Collection string
	Database   string
	URI        string
}

// Init creates a new mgo.Session with a strong consistency and a write mode of "majortiy".
func (m *Mongo) Init() (err error) {
	if session != nil {
		return
	}

	if session, err = mgo.Dial(m.URI); err != nil {
		return
	}

	session.EnsureSafe(&mgo.Safe{WMode: "majority"})
	session.SetMode(mgo.Strong, true)
	return
}

// GetAllDatasets retrieves all dataset documents from the configured collection.
func (m *Mongo) GetAllDatasets() (*models.DatasetResults, error) {
	s := session.Copy()
	defer s.Close()

	datasets := &models.DatasetResults{}

	iter := s.DB(m.Database).C(m.Collection).Find(nil).Iter()

	results := []models.Dataset{}
	if err := iter.All(&results); err != nil {
		return nil, err
	}

	datasets.Items = results

	return datasets, nil
}
