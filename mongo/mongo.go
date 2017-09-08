package mongo

import (
	"strconv"

	"github.com/ONSdigital/dp-dataset-api/api"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"

	"github.com/ONSdigital/dp-dataset-api/api-errors"
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

// GetDatasets retrieves all dataset documents
func (m *Mongo) GetDatasets() (*models.DatasetResults, error) {
	s := session.Copy()
	defer s.Close()

	datasets := &models.DatasetResults{}

	iter := s.DB(m.Database).C("datasets").Find(nil).Iter()

	results := []models.DatasetUpdate{}
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, api_errors.DatasetNotFound
		}
		return nil, err
	}

	items := mapResults(results)
	datasets.Items = items

	return datasets, nil
}

func mapResults(results []models.DatasetUpdate) []*models.Dataset {
	items := []*models.Dataset{}
	for _, item := range results {
		if item.Current == nil {
			continue
		}

		items = append(items, item.Current)
	}
	return items
}

// GetDataset retrieves a dataset document
func (m *Mongo) GetDataset(id string) (*models.DatasetUpdate, error) {
	s := session.Copy()
	defer s.Clone()
	var dataset models.DatasetUpdate
	err := s.DB(m.Database).C("datasets").Find(bson.M{"_id": id}).One(&dataset)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, api_errors.DatasetNotFound
		}
		return nil, err
	}

	return &dataset, nil
}

// GetEditions retrieves all edition documents for a dataset
func (m *Mongo) GetEditions(id string, selector interface{}) (*models.EditionResults, error) {
	s := session.Copy()
	defer s.Clone()
	iter := s.DB(m.Database).C("editions").Find(selector).Iter()

	var results []models.Edition
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, api_errors.EditionNotFound
		}
		return nil, err
	}

	if len(results) < 1 {
		return nil, api_errors.EditionNotFound
	}
	return &models.EditionResults{Items: results}, nil
}

// GetEdition retrieves an edition document for a dataset
func (m *Mongo) GetEdition(selector interface{}) (*models.Edition, error) {
	s := session.Copy()
	defer s.Clone()
	var edition models.Edition
	err := s.DB(m.Database).C("editions").Find(selector).One(&edition)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, api_errors.EditionNotFound
		}
		return nil, err
	}
	return &edition, nil
}

// GetNextVersion retrieves the latest version for an edition of a dataset
func (m *Mongo) GetNextVersion(datasetID, editionID string) (int, error) {
	s := session.Copy()
	defer s.Clone()
	var version models.Version
	var nextVersion int
	err := s.DB(m.Database).C("versions").Find(bson.M{"links.dataset.id": datasetID, "edition": editionID}).Sort("-version").One(&version)
	if err != nil {
		if err == mgo.ErrNotFound {
			return 1, nil
		}
		return nextVersion, err
	}
	currentVersion, err := strconv.Atoi(version.Version)
	if err != nil {
		log.ErrorC("Cannot convert version number to integer", err, nil)
		return nextVersion, err
	}

	nextVersion = currentVersion + 1

	return nextVersion, nil
}

// GetVersions retrieves all version documents for a dataset edition
func (m *Mongo) GetVersions(selector interface{}) (*models.VersionResults, error) {
	s := session.Copy()
	defer s.Clone()
	iter := s.DB(m.Database).C("versions").Find(selector).Iter()

	var results []models.Version
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, api_errors.VersionNotFound
		}
		return nil, err
	}

	if len(results) < 1 {
		return nil, api_errors.VersionNotFound
	}

	return &models.VersionResults{Items: results}, nil
}

// GetVersion retrieves a version document for a dataset edition
func (m *Mongo) GetVersion(selector interface{}) (*models.Version, error) {
	s := session.Copy()
	defer s.Clone()
	var version models.Version
	err := s.DB(m.Database).C("versions").Find(selector).One(&version)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, api_errors.VersionNotFound
		}
		return nil, err
	}
	return &version, nil
}

// UpsertDataset adds or overides an existing dataset document
func (m *Mongo) UpsertDataset(id string, update interface{}) (err error) {
	s := session.Copy()
	defer s.Close()

	_, err = s.DB(m.Database).C("datasets").UpsertId(id, update)
	return
}

// UpsertEdition adds or overides an existing edition document
func (m *Mongo) UpsertEdition(edition string, update interface{}) (err error) {
	s := session.Copy()
	defer s.Close()

	_, err = s.DB(m.Database).C("editions").Upsert(bson.M{"edition": edition}, update)
	return
}

// UpdateDataset updates an existing dataset document
func (m *Mongo) UpdateDataset(id string, update interface{}) (err error) {
	s := session.Copy()
	defer s.Close()

	err = s.DB(m.Database).C("dataset").UpdateId(id, update)
	return
}

// UpdateEdition updates an existing edition document
func (m *Mongo) UpdateEdition(id string, update interface{}) (err error) {
	s := session.Copy()
	defer s.Close()

	err = s.DB(m.Database).C("editions").UpdateId(id, update)
	return
}

// UpsertVersion adds or overides an existing version document
func (m *Mongo) UpsertVersion(id string, update interface{}) (err error) {
	s := session.Copy()
	defer s.Close()

	_, err = s.DB(m.Database).C("versions").UpsertId(id, update)
	return
}

// UpsertContact adds or overides an existing contact document
func (m *Mongo) UpsertContact(id string, update interface{}) (err error) {
	s := session.Copy()
	defer s.Close()

	_, err = s.DB(m.Database).C("contacts").UpsertId(id, update)
	return
}
