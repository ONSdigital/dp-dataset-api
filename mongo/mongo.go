package mongo

import (
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"strconv"
	"strings"
)

var _ store.Storer = &Mongo{}
var session *mgo.Session

// Mongo represents a simplistic MongoDB configuration.
type Mongo struct {
	Collection  string
	Database    string
	URI         string
	CodeListURL string
	DatasetURL  string
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
	defer iter.Close()

	results := []models.DatasetUpdate{}
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.DatasetNotFound
		}
		return nil, err
	}

	datasets.Items = mapResults(results)

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
			return nil, errs.DatasetNotFound
		}
		return nil, err
	}

	return &dataset, nil
}

// GetEditions retrieves all edition documents for a dataset
func (m *Mongo) GetEditions(id, state string) (*models.EditionResults, error) {
	s := session.Copy()
	defer s.Clone()

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
	s := session.Copy()
	defer s.Clone()

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

	nextVersion = version.Version + 1

	return nextVersion, nil
}

// GetVersions retrieves all version documents for a dataset edition
func (m *Mongo) GetVersions(id, editionID, state string) (*models.VersionResults, error) {
	s := session.Copy()
	defer s.Clone()

	selector := buildVersionsQuery(id, editionID, state)

	iter := s.DB(m.Database).C("versions").Find(selector).Iter()
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
	s := session.Copy()
	defer s.Clone()

	selector := buildVersionQuery(id, editionID, versionID, state)

	var version models.Version
	err := s.DB(m.Database).C("versions").Find(selector).One(&version)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.VersionNotFound
		}
		return nil, err
	}
	return &version, nil
}

func buildVersionQuery(id, editionID, versionID, state string) bson.M {
	var selector bson.M
	if state == "" {
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

// UpsertDataset adds or overides an existing dataset document
func (m *Mongo) UpsertDataset(id string, datasetDoc *models.DatasetUpdate) (err error) {
	s := session.Copy()
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
func (m *Mongo) UpsertEdition(editionID string, editionDoc *models.Edition) (err error) {
	s := session.Copy()
	defer s.Close()

	update := bson.M{
		"$set": editionDoc,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err = s.DB(m.Database).C("editions").Upsert(bson.M{"edition": editionID}, update)
	return
}

// UpdateDatasetWithAssociation updates an existing dataset document
func (m *Mongo) UpdateDatasetWithAssociation(id, state string, version *models.Version) (err error) {
	s := session.Copy()
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

	err = s.DB(m.Database).C("dataset").UpdateId(id, update)
	return
}

// UpdateEdition updates an existing edition document
func (m *Mongo) UpdateEdition(id, state string) (err error) {
	s := session.Copy()
	defer s.Close()

	update := bson.M{
		"$set": bson.M{
			"state": state,
		},
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	err = s.DB(m.Database).C("editions").UpdateId(id, update)
	return
}

// UpsertVersion adds or overides an existing version document
func (m *Mongo) UpsertVersion(id string, version *models.Version) (err error) {
	s := session.Copy()
	defer s.Close()

	update := bson.M{
		"$set": version,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	info, err := s.DB(m.Database).C("versions").UpsertId(id, update)
	if info.Updated == 0 {
		m.createDimensionsFromInstance(version.Links.Dataset.ID, version.Edition, version.Version, version.InstanceID)
	}
	return
}

// UpsertContact adds or overides an existing contact document
func (m *Mongo) UpsertContact(id string, update interface{}) (err error) {
	s := session.Copy()
	defer s.Close()

	_, err = s.DB(m.Database).C("contacts").UpsertId(id, update)
	return
}

// CreateDimensionsFromInstance adds multiple dimensions into mongo and relates the dimensions to a code list
func (m *Mongo) createDimensionsFromInstance(datasetID, editionID string, versionID int, instanceID string) error {
	s := session.Copy()
	defer s.Close()
	var instance models.Instance
	err := s.DB(m.Database).C(INSTANCE_COLLECTION).Find(bson.M{"id": instanceID}).One(&instance)
	if err != nil {
		return err
	}
	version := strconv.Itoa(versionID)

	for _, column := range *instance.Headers {
		if !strings.Contains(column, "V4_") && strings.Contains(column, "_") {

			split := strings.Split(column, "_")
			name := split[0]
			codeID := split[1]
			time := time.Now().UTC()
			dimension := models.Dimension{}
			dimension.Name = name
			dimension.Links.CodeList = models.LinkObject{ID: codeID, HRef: m.CodeListURL + "/code-lists/" + codeID}
			dimension.Links.Dataset = models.LinkObject{ID: datasetID, HRef: m.DatasetURL + "/datasets/" + datasetID}
			dimension.Links.Edition = models.LinkObject{ID: editionID, HRef: m.DatasetURL + "/datasets/" + datasetID + "/editions/" + editionID}
			dimension.Links.Version = models.LinkObject{ID: version, HRef: m.DatasetURL + "/datasets/" + datasetID + "/editions/" + editionID + "/versions/" + version}
			dimension.LastUpdated = &time

			err = s.DB(m.Database).C("dimensions").Insert(&dimension)
			if err != nil {
				return nil
			}
		}
	}
	return nil
}

// GetDimensions returns a list of all dimensions from a dataset
func (m *Mongo) GetDimensions(datasetID, editionID, versionID string) (*models.DatasetDimensionResults, error) {
	//version, err := strconv.ParseInt(versionID, 10, 64)
	s := session.Copy()
	defer s.Close()
	iter := s.DB(m.Database).C("dimensions").Find(bson.M{"links.dataset.id": datasetID, "links.edition.id": editionID, "links.version.id": versionID}).
		Select(bson.M{"last_updated": 0}).
		Iter()
	defer iter.Close()
	var results []models.Dimension
	err := iter.All(&results)
	if err != nil {
		return nil, err
	}
	return &models.DatasetDimensionResults{Items: results}, nil
}
