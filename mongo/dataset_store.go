package mongo

import (
	"errors"
	"strconv"
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var _ store.Storer = &Mongo{}

// Mongo represents a simplistic MongoDB configuration.
type Mongo struct {
	CodeListURL string
	Collection  string
	Database    string
	DatasetURL  string
	Session     *mgo.Session
	URI         string
}

// Init creates a new mgo.Session with a strong consistency and a write mode of "majortiy".
func (m *Mongo) Init() (session *mgo.Session, err error) {
	if session != nil {
		return nil, errors.New("session already exists")
	}

	if session, err = mgo.Dial(m.URI); err != nil {
		return nil, err
	}

	session.EnsureSafe(&mgo.Safe{WMode: "majority"})
	session.SetMode(mgo.Strong, true)
	return session, nil
}

// GetDatasets retrieves all dataset documents
func (m *Mongo) GetDatasets() ([]models.DatasetUpdate, error) {
	s := m.Session.Copy()
	defer s.Close()

	iter := s.DB(m.Database).C("datasets").Find(nil).Iter()
	defer iter.Close()

	results := []models.DatasetUpdate{}
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.DatasetNotFound
		}
		return nil, err
	}

	return results, nil
}

// GetDataset retrieves a dataset document
func (m *Mongo) GetDataset(id string) (*models.DatasetUpdate, error) {
	s := m.Session.Copy()
	defer s.Close()
	var dataset models.DatasetUpdate
	err := s.DB(m.Database).C("datasets").Find(bson.M{"_id": id}).One(&dataset)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.DatasetNotFound
		}
		return nil, err
	}

	return &dataset, nil
}

// GetEditions retrieves all edition documents for a dataset
func (m *Mongo) GetEditions(id, state string) (*models.EditionResults, error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := buildEditionsQuery(id, state)

	iter := s.DB(m.Database).C("editions").Find(selector).Iter()
	defer iter.Close()

	var results []models.Edition
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.EditionNotFound
		}
		return nil, err
	}

	if len(results) < 1 {
		return nil, errs.EditionNotFound
	}
	return &models.EditionResults{Items: results}, nil
}

func buildEditionsQuery(id, state string) bson.M {
	var selector bson.M
	if state != "" {
		selector = bson.M{
			"links.dataset.id": id,
			"state":            state,
		}
	} else {
		selector = bson.M{
			"links.dataset.id": id,
		}
	}

	return selector
}

// GetEdition retrieves an edition document for a dataset
func (m *Mongo) GetEdition(id, editionID, state string) (*models.Edition, error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := buildEditionQuery(id, editionID, state)

	var edition models.Edition
	err := s.DB(m.Database).C("editions").Find(selector).One(&edition)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.EditionNotFound
		}
		return nil, err
	}
	return &edition, nil
}

func buildEditionQuery(id, editionID, state string) bson.M {
	var selector bson.M
	if state == "" {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
		}
	} else {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"state":            state,
		}
	}

	return selector
}

// GetNextVersion retrieves the latest version for an edition of a dataset
func (m *Mongo) GetNextVersion(datasetID, edition string) (int, error) {
	s := m.Session.Copy()
	defer s.Close()
	var version models.Version
	var nextVersion int

	selector := bson.M{
		"links.dataset.id": datasetID,
		"edition":          edition,
	}

	// Results are sorted in reverse order to get latest version
	err := s.DB(m.Database).C("instances").Find(selector).Sort("-version").One(&version)
	if err != nil {
		if err == mgo.ErrNotFound {
			return 1, nil
		}
		return nextVersion, err
	}

	nextVersion = version.Version + 1

	return nextVersion, nil
}

// GetVersions retrieves all version documents for a dataset edition
func (m *Mongo) GetVersions(id, editionID, state string) (*models.VersionResults, error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := buildVersionsQuery(id, editionID, state)

	iter := s.DB(m.Database).C("instances").Find(selector).Iter()
	defer iter.Close()

	var results []models.Version
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.VersionNotFound
		}
		return nil, err
	}

	if len(results) < 1 {
		return nil, errs.VersionNotFound
	}

	for i := 0; i < len(results); i++ {
		results[i].Links.Self.HRef = results[i].Links.Version.HRef
	}

	return &models.VersionResults{Items: results}, nil
}

func buildVersionsQuery(id, editionID, state string) bson.M {
	var selector bson.M
	if state == "" {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
		}
	} else {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"state":            state,
		}
	}

	return selector
}

// GetVersion retrieves a version document for a dataset edition
func (m *Mongo) GetVersion(id, editionID, versionID, state string) (*models.Version, error) {
	s := m.Session.Copy()
	defer s.Close()

	versionNumber, err := strconv.Atoi(versionID)
	if err != nil {
		return nil, err
	}
	selector := buildVersionQuery(id, editionID, state, versionNumber)

	var version models.Version
	err = s.DB(m.Database).C("instances").Find(selector).One(&version)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.VersionNotFound
		}
		return nil, err
	}
	return &version, nil
}

func buildVersionQuery(id, editionID, state string, versionID int) bson.M {
	var selector bson.M
	if state != "published" {
		selector = bson.M{
			"links.dataset.id": id,
			"version":          versionID,
			"edition":          editionID,
		}
	} else {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"version":          versionID,
			"state":            state,
		}
	}

	return selector
}

// UpdateDataset updates an existing dataset document
func (m *Mongo) UpdateDataset(id string, dataset *models.Dataset) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	updates := createDatasetUpdateQuery(dataset)
	err = s.DB(m.Database).C("datasets").UpdateId(id, bson.M{"$set": updates, "$setOnInsert": bson.M{"next.last_updated": time.Now()}})
	return
}

func createDatasetUpdateQuery(dataset *models.Dataset) bson.M {
	updates := make(bson.M, 0)

	if dataset.CollectionID != "" {
		updates["next.collection_id"] = dataset.CollectionID
	}

	if dataset.Contacts != nil {
		updates["next.contacts"] = dataset.Contacts
	}

	if dataset.Description != "" {
		updates["next.description"] = dataset.Description
	}

	if dataset.Keywords != nil {
		updates["next.keywords"] = dataset.Keywords
	}

	if dataset.License != "" {
		updates["next.license"] = dataset.License
	}

	if dataset.Methodologies != nil {
		updates["next.methodologies"] = dataset.Methodologies
	}

	if dataset.NationalStatistic != false {
		updates["next.national_statistic"] = dataset.NationalStatistic
	}

	if dataset.NextRelease != "" {
		updates["next.next_release"] = dataset.NextRelease
	}

	if dataset.Publications != nil {
		updates["next.publications"] = dataset.Publications
	}

	if dataset.Publisher.HRef != "" {
		updates["next.publisher.href"] = dataset.Publisher.HRef
	}

	if dataset.Publisher.Name != "" {
		updates["next.publisher.name"] = dataset.Publisher.Name
	}

	if dataset.Publisher.Type != "" {
		updates["next.publisher.type"] = dataset.Publisher.Type
	}

	if dataset.QMI.Description != "" {
		updates["next.qmi.description"] = dataset.QMI.Description
	}

	if dataset.QMI.HRef != "" {
		updates["next.qmi.href"] = dataset.QMI.HRef
	}

	if dataset.QMI.Title != "" {
		updates["next.qmi.title"] = dataset.QMI.Title
	}

	if dataset.RelatedDatasets != nil {
		updates["next.related_datasets"] = dataset.RelatedDatasets
	}

	if dataset.ReleaseFrequency != "" {
		updates["next.release_frequency"] = dataset.ReleaseFrequency
	}

	if dataset.Theme != "" {
		updates["next.theme"] = dataset.Theme
	}

	if dataset.Title != "" {
		updates["next.title"] = dataset.Title
	}

	if dataset.URI != "" {
		updates["next.uri"] = dataset.URI
	}

	return updates
}

// UpdateDatasetWithAssociation updates an existing dataset document with collection data
func (m *Mongo) UpdateDatasetWithAssociation(id, state string, version *models.Version) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	update := bson.M{
		"$set": bson.M{
			"next.state":                     state,
			"next.collection_id":             version.CollectionID,
			"next.links.latest_version.link": version.Links.Self,
			"next.links.latest_version.id":   version.ID,
			"next.last_updated":              time.Now(),
		},
	}

	err = s.DB(m.Database).C("datasets").UpdateId(id, update)
	return
}

// UpdateEdition updates an existing edition document
func (m *Mongo) UpdateEdition(datasetID, edition, state string) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	update := bson.M{
		"$set": bson.M{
			"state": state,
		},
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	err = s.DB(m.Database).C("editions").Update(bson.M{"links.dataset.id": datasetID, "edition": edition}, update)
	return
}

// UpdateVersion updates an existing version document
func (m *Mongo) UpdateVersion(id string, version *models.Version) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	updates := createVersionUpdateQuery(version)

	err = s.DB(m.Database).C("instances").Update(bson.M{"id": id}, bson.M{"$set": updates, "$setOnInsert": bson.M{"last_updated": time.Now()}})
	return
}

func createVersionUpdateQuery(version *models.Version) bson.M {
	updates := make(bson.M, 0)

	if version.CollectionID != "" {
		updates["collection_id"] = version.CollectionID
	}

	if version.ReleaseDate != "" {
		updates["release_date"] = version.ReleaseDate
	}

	if version.Spatial != "" {
		updates["spatial"] = version.Spatial
	}

	if version.State != "" {
		updates["state"] = version.State
	}

	if version.Spatial != "" {
		updates["temporal"] = version.Temporal
	}

	return updates
}

// UpsertDataset adds or overides an existing dataset document
func (m *Mongo) UpsertDataset(id string, datasetDoc *models.DatasetUpdate) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	update := bson.M{
		"$set": datasetDoc,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err = s.DB(m.Database).C("datasets").UpsertId(id, update)
	return
}

// UpsertEdition adds or overides an existing edition document
func (m *Mongo) UpsertEdition(datasetID, edition string, editionDoc *models.Edition) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := bson.M{
		"edition":          edition,
		"links.dataset.id": datasetID,
	}

	update := bson.M{
		"$set": editionDoc,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err = s.DB(m.Database).C("editions").Upsert(selector, update)
	return
}

// UpsertVersion adds or overrides an existing version document
func (m *Mongo) UpsertVersion(id string, version *models.Version) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	update := bson.M{
		"$set": version,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err = s.DB(m.Database).C("instances").UpsertId(id, update)
	return err
}

// UpsertContact adds or overides an existing contact document
func (m *Mongo) UpsertContact(id string, update interface{}) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	_, err = s.DB(m.Database).C("contacts").UpsertId(id, update)
	return
}

func (m *Mongo) CheckDatasetExists(id, state string) error {
	s := m.Session.Copy()
	defer s.Close()

	var query bson.M
	if state == "" {
		query = bson.M{
			"_id": id,
		}
	} else {
		query = bson.M{
			"_id":           id,
			"current.state": state,
		}
	}

	count, err := s.DB(m.Database).C("datasets").Find(query).Count()
	if err != nil {
		return err
	}

	if count == 0 {
		return errs.DatasetNotFound
	}

	return nil
}

func (m *Mongo) CheckEditionExists(id, editionID, state string) error {
	s := m.Session.Copy()
	defer s.Close()

	var query bson.M
	if state == "" {
		query = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
		}
	} else {
		query = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"state":            state,
		}
	}

	count, err := s.DB(m.Database).C("editions").Find(query).Count()
	if err != nil {
		return err
	}

	if count == 0 {
		return errs.EditionNotFound
	}

	return nil
}
