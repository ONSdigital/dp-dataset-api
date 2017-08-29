package api

import (
	"github.com/ONSdigital/dp-dataset-api/models"
	"gopkg.in/mgo.v2"
)

// DataStore provides a datastore.Backend interface used to store, retrieve, remove or update datasets
type DataStore struct {
	Backend Backend
}

// Backend represents basic data access via Get, Remove and Upsert methods.
type Backend interface {
	GetAllDatasets() (*models.DatasetResults, error)
	UpsertDataset(id interface{}, update interface{}) error
	UpsertEdition(id interface{}, update interface{}) error
	UpsertVersion(id interface{}, update interface{}) error
	UpsertContact(id interface{}, update interface{}) error
}

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

// GetAllDatasets retrieves all dataset documents
func (m *Mongo) GetAllDatasets() (*models.DatasetResults, error) {
	s := session.Copy()
	defer s.Close()

	datasets := &models.DatasetResults{}

	iter := s.DB(m.Database).C("datasets").Find(nil).Iter()

	results := []models.Dataset{}
	if err := iter.All(&results); err != nil {
		return nil, err
	}

	datasets.Items = results

	return datasets, nil
}

// UpsertDataset adds or overides an existing dataset document
func (m *Mongo) UpsertDataset(id interface{}, update interface{}) (err error) {
	s := session.Copy()
	defer s.Close()

	_, err = s.DB(m.Database).C("datasets").UpsertId(id, update)
	return
}

// UpsertEdition adds or overides an existing edition document
func (m *Mongo) UpsertEdition(id interface{}, update interface{}) (err error) {
	s := session.Copy()
	defer s.Close()

	_, err = s.DB(m.Database).C("editions").UpsertId(id, update)
	return
}

// UpsertVersion adds or overides an existing version document
func (m *Mongo) UpsertVersion(id interface{}, update interface{}) (err error) {
	s := session.Copy()
	defer s.Close()

	_, err = s.DB(m.Database).C("versions").UpsertId(id, update)
	return
}

// UpsertContact adds or overides an existing contact document
func (m *Mongo) UpsertContact(id interface{}, update interface{}) (err error) {
	s := session.Copy()
	defer s.Close()

	_, err = s.DB(m.Database).C("contacts").UpsertId(id, update)
	return
}
