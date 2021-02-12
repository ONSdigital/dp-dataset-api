package mongo

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	dpmongo "github.com/ONSdigital/dp-mongodb"
	dpMongoHealth "github.com/ONSdigital/dp-mongodb/health"
	"github.com/ONSdigital/log.go/log"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// Mongo represents a simplistic MongoDB configuration.
type Mongo struct {
	CodeListURL    string
	Collection     string
	Database       string
	DatasetURL     string
	Session        *mgo.Session
	URI            string
	lastPingTime   time.Time
	lastPingResult error
	healthClient   *dpMongoHealth.CheckMongoClient
}

const (
	editionsCollection = "editions"
)

// Init creates a new mgo.Session with a strong consistency and a write mode of "majortiy"; and initialises the mongo health client.
func (m *Mongo) Init() (err error) {
	if m.Session != nil {
		return errors.New("session already exists")
	}

	// Create session
	if m.Session, err = mgo.Dial(m.URI); err != nil {
		return err
	}
	m.Session.EnsureSafe(&mgo.Safe{WMode: "majority"})
	m.Session.SetMode(mgo.Strong, true)

	databaseCollectionBuilder := make(map[dpMongoHealth.Database][]dpMongoHealth.Collection)
	databaseCollectionBuilder[(dpMongoHealth.Database)(m.Database)] = []dpMongoHealth.Collection{(dpMongoHealth.Collection)(m.Collection), (dpMongoHealth.Collection)(editionsCollection), (dpMongoHealth.Collection)(instanceCollection), (dpMongoHealth.Collection)(dimensionOptions)}

	// Create client and healthclient from session
	client := dpMongoHealth.NewClientWithCollections(m.Session, databaseCollectionBuilder)
	m.healthClient = &dpMongoHealth.CheckMongoClient{
		Client:      *client,
		Healthcheck: client.Healthcheck,
	}

	return nil
}

// Close represents mongo session closing within the context deadline
func (m *Mongo) Close(ctx context.Context) error {
	if m.Session == nil {
		return errors.New("cannot close a mongoDB connection without a valid session")
	}
	return dpmongo.Close(ctx, m.Session)
}

// Checker is called by the healthcheck library to check the health state of this mongoDB instance
func (m *Mongo) Checker(ctx context.Context, state *healthcheck.CheckState) error {
	return m.healthClient.Checker(ctx, state)
}

// GetDatasets retrieves all dataset documents
func (m *Mongo) GetDatasets(ctx context.Context, offset, limit int, authorised bool) (*models.DatasetUpdateResults, error) {
	s := m.Session.Copy()
	defer s.Close()

	var q *mgo.Query

	if authorised {
		q = s.DB(m.Database).C("datasets").Find(nil)
	} else {
		q = s.DB(m.Database).C("datasets").Find(bson.M{"current": bson.M{"$exists": true}})
	}

	iter := q.Sort().Skip(offset).Limit(limit).Iter()

	defer func() {
		err := iter.Close()
		if err != nil {
			log.Event(ctx, "error closing iterator", log.ERROR, log.Error(err))
		}
	}()

	totalCount, err := q.Count()
	if err != nil {
		log.Event(ctx, "error counting items", log.ERROR, log.Error(err))
		if err == mgo.ErrNotFound {
			return &models.DatasetUpdateResults{
				Items:      []models.DatasetUpdate{},
				Count:      0,
				TotalCount: 0,
				Offset:     offset,
				Limit:      limit,
			}, nil
		}
		return nil, err
	}

	values := []models.DatasetUpdate{}

	if limit > 0 {
		if err := iter.All(&values); err != nil {
			if err == mgo.ErrNotFound {
				return &models.DatasetUpdateResults{
					Items:      values,
					Count:      0,
					TotalCount: totalCount,
					Offset:     offset,
					Limit:      limit,
				}, nil
			}
			return nil, err
		}
	}

	return &models.DatasetUpdateResults{
		Items:      values,
		Count:      len(values),
		TotalCount: totalCount,
		Offset:     offset,
		Limit:      limit,
	}, nil
}

// GetDataset retrieves a dataset document
func (m *Mongo) GetDataset(id string) (*models.DatasetUpdate, error) {
	s := m.Session.Copy()
	defer s.Close()
	var dataset models.DatasetUpdate
	err := s.DB(m.Database).C("datasets").Find(bson.M{"_id": id}).One(&dataset)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.ErrDatasetNotFound
		}
		return nil, err
	}

	return &dataset, nil
}

// GetEditions retrieves all edition documents for a dataset
func (m *Mongo) GetEditions(ctx context.Context, id, state string, offset, limit int, authorised bool) (*models.EditionUpdateResults, error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := buildEditionsQuery(id, state, authorised)
	q := s.DB(m.Database).C(editionsCollection).Find(selector)

	totalCount, err := q.Count()
	if err != nil {
		log.Event(ctx, "error counting items", log.ERROR, log.Error(err))
		if err == mgo.ErrNotFound {
			return &models.EditionUpdateResults{
				Items:      []*models.EditionUpdate{},
				Count:      0,
				TotalCount: 0,
				Offset:     offset,
				Limit:      limit,
			}, nil
		}
		return nil, err
	}

	if totalCount < 1 {
		return nil, errs.ErrEditionNotFound
	}

	iter := q.Sort().Skip(offset).Limit(limit).Iter()
	defer func() {
		err := iter.Close()
		if err != nil {
			log.Event(ctx, "error closing edition iterator", log.ERROR, log.Error(err), log.Data{"selector": selector})
		}
	}()

	var results []*models.EditionUpdate

	if limit > 0 {
		if err := iter.All(&results); err != nil {
			if err == mgo.ErrNotFound {
				return &models.EditionUpdateResults{
					Items:      []*models.EditionUpdate{},
					Count:      0,
					TotalCount: totalCount,
					Offset:     offset,
					Limit:      limit,
				}, nil
			}
			return nil, err
		}
	}

	return &models.EditionUpdateResults{
		Items:      results,
		Count:      len(results),
		TotalCount: totalCount,
		Offset:     offset,
		Limit:      limit,
	}, nil
}

func buildEditionsQuery(id, state string, authorised bool) bson.M {

	// all queries must get the dataset by id
	selector := bson.M{
		"next.links.dataset.id": id,
	}

	// non-authorised queries require that the current edition must exist
	if !authorised {
		selector["current"] = bson.M{"$exists": true}
	}

	// if state is required, then we need to query by state
	if state != "" {
		selector["current.state"] = state
	}

	return selector
}

// GetEdition retrieves an edition document for a dataset
func (m *Mongo) GetEdition(id, editionID, state string) (*models.EditionUpdate, error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := buildEditionQuery(id, editionID, state)

	var edition models.EditionUpdate
	err := s.DB(m.Database).C(editionsCollection).Find(selector).One(&edition)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.ErrEditionNotFound
		}
		return nil, err
	}
	return &edition, nil
}

func buildEditionQuery(id, editionID, state string) bson.M {
	var selector bson.M
	if state != "" {
		selector = bson.M{
			"current.links.dataset.id": id,
			"current.edition":          editionID,
			"current.state":            state,
		}
	} else {
		selector = bson.M{
			"next.links.dataset.id": id,
			"next.edition":          editionID,
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
func (m *Mongo) GetVersions(ctx context.Context, id, editionID, state string, offset, limit int) (*models.VersionResults, error) {
	s := m.Session.Copy()
	defer s.Close()

	var q *mgo.Query

	selector := buildVersionsQuery(id, editionID, state)

	q = s.DB(m.Database).C("instances").Find(selector)
	totalCount, err := q.Count()
	if err != nil {
		log.Event(ctx, "error counting items", log.ERROR, log.Error(err))
		if err == mgo.ErrNotFound {
			return &models.VersionResults{
				Items:      []models.Version{},
				Count:      0,
				TotalCount: 0,
				Offset:     offset,
				Limit:      limit,
			}, nil
		}
		return nil, err
	}

	iter := q.Sort("-last_updated").Skip(offset).Limit(limit).Iter()
	defer func() {
		err := iter.Close()
		if err != nil {
			log.Event(ctx, "error closing instance iterator", log.ERROR, log.Error(err), log.Data{"selector": selector})
		}
	}()

	var results []models.Version

	if limit > 0 {
		if err := iter.All(&results); err != nil {
			if err == mgo.ErrNotFound {
				return &models.VersionResults{
					Items:      results,
					Count:      0,
					TotalCount: totalCount,
					Offset:     offset,
					Limit:      limit,
				}, nil
			}
			return nil, err
		}
	}

	if len(results) < 1 {
		return nil, errs.ErrVersionNotFound
	}

	for i := 0; i < len(results); i++ {

		results[i].Links.Self.HRef = results[i].Links.Version.HRef
	}

	return &models.VersionResults{
		Items:      results,
		Count:      len(results),
		TotalCount: totalCount,
		Offset:     offset,
		Limit:      limit,
	}, nil
}

func buildVersionsQuery(id, editionID, state string) bson.M {
	var selector bson.M
	if state == "" {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"$or": []interface{}{
				bson.M{"state": models.EditionConfirmedState},
				bson.M{"state": models.AssociatedState},
				bson.M{"state": models.PublishedState},
			},
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
			return nil, errs.ErrVersionNotFound
		}
		return nil, err
	}
	return &version, nil
}

func buildVersionQuery(id, editionID, state string, versionID int) bson.M {
	var selector bson.M
	if state != models.PublishedState {
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
func (m *Mongo) UpdateDataset(ctx context.Context, id string, dataset *models.Dataset, currentState string) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	updates := createDatasetUpdateQuery(ctx, id, dataset, currentState)
	update := bson.M{"$set": updates, "$setOnInsert": bson.M{"next.last_updated": time.Now()}}
	if err = s.DB(m.Database).C("datasets").UpdateId(id, update); err != nil {
		if err == mgo.ErrNotFound {
			return errs.ErrDatasetNotFound
		}
		return err
	}

	return nil
}

func createDatasetUpdateQuery(ctx context.Context, id string, dataset *models.Dataset, currentState string) bson.M {
	updates := make(bson.M)

	log.Event(ctx, "building update query for dataset resource", log.INFO, log.Data{"dataset_id": id, "dataset": dataset, "updates": updates})

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

	if dataset.Links != nil {
		if dataset.Links.AccessRights != nil {
			if dataset.Links.AccessRights.HRef != "" {
				updates["next.links.access_rights.href"] = dataset.Links.AccessRights.HRef
			}
		}

		if dataset.Links.Taxonomy != nil {
			if dataset.Links.Taxonomy.HRef != "" {
				updates["next.links.taxonomy.href"] = dataset.Links.Taxonomy.HRef
			}
		}
	}

	if dataset.Methodologies != nil {
		updates["next.methodologies"] = dataset.Methodologies
	}

	if dataset.NationalStatistic != nil {
		updates["next.national_statistic"] = dataset.NationalStatistic
	}

	if dataset.NextRelease != "" {
		updates["next.next_release"] = dataset.NextRelease
	}

	if dataset.Publications != nil {
		updates["next.publications"] = dataset.Publications
	}

	if dataset.Publisher != nil {
		if dataset.Publisher.HRef != "" {
			updates["next.publisher.href"] = dataset.Publisher.HRef
		}

		if dataset.Publisher.Name != "" {
			updates["next.publisher.name"] = dataset.Publisher.Name
		}

		if dataset.Publisher.Type != "" {
			updates["next.publisher.type"] = dataset.Publisher.Type
		}
	}

	if dataset.QMI != nil {
		updates["next.qmi.description"] = dataset.QMI.Description
		updates["next.qmi.href"] = dataset.QMI.HRef
		updates["next.qmi.title"] = dataset.QMI.Title
	}

	if dataset.RelatedDatasets != nil {
		updates["next.related_datasets"] = dataset.RelatedDatasets
	}

	if dataset.ReleaseFrequency != "" {
		updates["next.release_frequency"] = dataset.ReleaseFrequency
	}

	if dataset.State != "" {
		updates["next.state"] = dataset.State
	} else {
		if currentState == models.PublishedState {
			updates["next.state"] = models.CreatedState
		}
	}

	if dataset.Theme != "" {
		updates["next.theme"] = dataset.Theme
	}

	if dataset.Title != "" {
		updates["next.title"] = dataset.Title
	}

	if dataset.UnitOfMeasure != "" {
		updates["next.unit_of_measure"] = dataset.UnitOfMeasure
	}

	if dataset.URI != "" {
		updates["next.uri"] = dataset.URI
	}

	if dataset.Type != "" {
		updates["next.type"] = dataset.Type
	}

	if dataset.NomisReferenceURL != "" {
		updates["next.nomis_reference_url"] = dataset.NomisReferenceURL
	}

	if err := models.ValidateDataset(ctx, dataset); err != nil {
		log.Event(ctx, "failed validation check to create dataset", log.ERROR, log.Error(err))
		return nil
	}

	log.Event(ctx, "built update query for dataset resource", log.INFO, log.Data{"dataset_id": id, "dataset": dataset, "updates": updates})

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
			"next.links.latest_version.href": version.Links.Version.HRef,
			"next.links.latest_version.id":   version.Links.Version.ID,
			"next.last_updated":              time.Now(),
		},
	}

	err = s.DB(m.Database).C("datasets").UpdateId(id, update)
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
	setUpdates := make(bson.M)

	/*
		Where updating a version to detached state:
		1.) explicitly set version number to nil
		2.) remove collectionID
	*/
	if version.State == models.DetachedState {
		setUpdates["collection_id"] = nil
		setUpdates["version"] = nil
	} else {
		if version.CollectionID != "" {
			setUpdates["collection_id"] = version.CollectionID
		}
	}

	if version.Alerts != nil {
		setUpdates["alerts"] = version.Alerts
	}

	if version.Downloads != nil {
		setUpdates["downloads"] = version.Downloads
	}

	if version.LatestChanges != nil {
		setUpdates["latest_changes"] = version.LatestChanges
	}

	if version.Links != nil {
		if version.Links.Spatial != nil {
			if version.Links.Spatial.HRef != "" {
				setUpdates["links.spatial.href"] = version.Links.Spatial.HRef
			}
		}
	}

	if version.ReleaseDate != "" {
		setUpdates["release_date"] = version.ReleaseDate
	}

	if version.State != "" {
		setUpdates["state"] = version.State
	}

	if version.Temporal != nil {
		setUpdates["temporal"] = version.Temporal
	}

	if version.UsageNotes != nil {
		setUpdates["usage_notes"] = version.UsageNotes
	}

	return setUpdates
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
func (m *Mongo) UpsertEdition(datasetID, edition string, editionDoc *models.EditionUpdate) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := bson.M{
		"next.edition":          edition,
		"next.links.dataset.id": datasetID,
	}

	editionDoc.Next.LastUpdated = time.Now()

	update := bson.M{
		"$set": editionDoc,
	}

	_, err = s.DB(m.Database).C(editionsCollection).Upsert(selector, update)
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

// CheckDatasetExists checks that the dataset exists
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
		return errs.ErrDatasetNotFound
	}

	return nil
}

// CheckEditionExists checks that the edition of a dataset exists
func (m *Mongo) CheckEditionExists(id, editionID, state string) error {
	s := m.Session.Copy()
	defer s.Close()

	var query bson.M
	if state == "" {
		query = bson.M{
			"next.links.dataset.id": id,
			"next.edition":          editionID,
		}
	} else {
		query = bson.M{
			"current.links.dataset.id": id,
			"current.edition":          editionID,
			"current.state":            state,
		}
	}

	count, err := s.DB(m.Database).C(editionsCollection).Find(query).Count()
	if err != nil {
		return err
	}

	if count == 0 {
		return errs.ErrEditionNotFound
	}

	return nil
}

// Ping the mongodb database
func (m *Mongo) Ping(ctx context.Context) (time.Time, error) {
	if time.Since(m.lastPingTime) < 1*time.Second {
		return m.lastPingTime, m.lastPingResult
	}

	s := m.Session.Copy()
	defer s.Close()

	m.lastPingTime = time.Now()
	pingDoneChan := make(chan error)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		log.Event(ctx, "db ping", log.INFO)
		err := s.Ping()
		if err != nil {
			log.Event(ctx, "Ping mongo", log.ERROR, log.Error(err))
		}
		pingDoneChan <- err
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(pingDoneChan)
	}()

	select {
	case err := <-pingDoneChan:
		m.lastPingResult = err
	case <-ctx.Done():
		m.lastPingResult = ctx.Err()
	}
	return m.lastPingTime, m.lastPingResult
}

// DeleteDataset deletes an existing dataset document
func (m *Mongo) DeleteDataset(id string) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	if err = s.DB(m.Database).C("datasets").RemoveId(id); err != nil {
		if err == mgo.ErrNotFound {
			return errs.ErrDatasetNotFound
		}
		return err
	}

	return nil
}

// DeleteEdition deletes an existing edition document
func (m *Mongo) DeleteEdition(id string) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	if err = s.DB(m.Database).C("editions").Remove(bson.D{{Name: "id", Value: id}}); err != nil {
		if err == mgo.ErrNotFound {
			return errs.ErrEditionNotFound
		}
		return err
	}

	return nil
}
