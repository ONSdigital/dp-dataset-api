package mongo

import (
	"github.com/ONSdigital/dp-dataset-api/api"
	"github.com/ONSdigital/dp-dataset-api/models"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var _ api.Backend = &Mongo{}
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
func (m *Mongo) GetDatasets() (*models.DatasetResults, error) {
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

func (m *Mongo) GetDataset(id string) (*models.Dataset, error) {
	s := session.Copy()
	defer s.Clone()
	var dataset models.Dataset
	err := s.DB(m.Database).C("datasets").Find(bson.M{"links.self": "/datasets/" + id}).One(&dataset)
	if err != nil {
		return nil, err
	}
	return &dataset, nil
}

func (m *Mongo) GetEditions(id string) (*models.EditionResults, error) {
	s := session.Copy()
	defer s.Clone()
	iter := s.DB(m.Database).C("editions").Find(bson.M{"links.dataset": "/datasets/" + id}).Iter()

	var results []models.Edition
	if err := iter.All(&results); err != nil {
		return nil, err
	}
	return &models.EditionResults{Items: results}, nil
}

func (m *Mongo) GetEdition(datasetID, editionID string) (*models.Edition, error) {
	s := session.Copy()
	defer s.Clone()
	var edition models.Edition
	link := "/datasets/" + datasetID + "/editions/" + editionID
	err := s.DB(m.Database).C("editions").Find(bson.M{"links.self": link}).One(&edition)
	if err != nil {
		return nil, err
	}
	return &edition, nil
}
func (m *Mongo) GetVersions(datasetID, editionID string) (*models.VersionResults, error) {
	s := session.Copy()
	defer s.Clone()
	link := "/datasets/" + datasetID + "/editions/" + editionID
	iter := s.DB(m.Database).C("versions").Find(bson.M{"links.edition": link}).Iter()

	var results []models.Version
	if err := iter.All(&results); err != nil {
		return nil, err
	}
	return &models.VersionResults{Items: results}, nil
}
func (m *Mongo) GetVersion(datasetID, editionID, versionID string) (*models.Version, error) {
	s := session.Copy()
	defer s.Clone()
	var version models.Version
	link := "/datasets/" + datasetID + "/editions/" + editionID + "/versions/" + versionID
	err := s.DB(m.Database).C("versions").Find(bson.M{"links.self": link}).One(&version)
	if err != nil {
		return nil, err
	}
	return &version, nil
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
