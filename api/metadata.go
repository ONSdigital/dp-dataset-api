package api

import (
	"encoding/json"
	"io"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/utils"
	dphttp "github.com/ONSdigital/dp-net/v3/http"
	"github.com/ONSdigital/dp-net/v3/links"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
func (api *DatasetAPI) getMetadata(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	b, err := func() ([]byte, error) {
		versionID, err := models.ParseAndValidateVersionNumber(ctx, version)
		if err != nil {
			log.Error(ctx, "failed due to invalid version request", err, logData)
			return nil, err
		}

		datasetDoc, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
		if err != nil {
			log.Error(ctx, "getMetadata endpoint: get datastore.getDataset returned an error", err, logData)
			return nil, err
		}

		var versionDoc *models.Version

		doc := datasetDoc.Current
		if doc == nil {
			if datasetDoc.Next == nil {
				return nil, errors.New("invalid dataset doc: no 'current' or 'next' found")
			}
			doc = datasetDoc.Next
		}

		datasetType, err := models.GetDatasetType(doc.Type)
		if err != nil {
			log.Error(ctx, "invalid dataset type", err, logData)
			return nil, err
		}

		isStaticDataset := (datasetType == models.Static)

		authorised := api.authenticate(r, logData)

		if isStaticDataset {
			versionDoc, err = api.dataStore.Backend.GetVersionStatic(ctx, datasetID, edition, versionID, "")
		} else {
			versionDoc, err = api.dataStore.Backend.GetVersion(ctx, datasetID, edition, versionID, "")
		}

		if err != nil {
			if err == errs.ErrVersionNotFound {
				log.Error(ctx, "getMetadata endpoint: failed to find version for dataset edition", err, logData)
				return nil, errs.ErrMetadataVersionNotFound
			}
			log.Error(ctx, "getMetadata endpoint: get version returned an error", err, logData)
			return nil, err
		}

		state := versionDoc.State

		// if the requested version is not yet published and the user is unauthorised, return a 404
		if !authorised && versionDoc.State != models.PublishedState {
			log.Error(ctx, "getMetadata endpoint: unauthorised user requested unpublished version, returning 404", errs.ErrUnauthorised, logData)
			return nil, errs.ErrUnauthorised
		}

		// if request is not authenticated but the version is published, restrict dataset access to only published resources
		if !authorised {
			// Check for current sub document
			if datasetDoc.Current == nil || datasetDoc.Current.State != models.PublishedState {
				logData["dataset"] = datasetDoc.Current
				log.Error(ctx, "getMetadata endpoint: caller not is authorised and dataset but currently unpublished", errors.New("document is not currently published"), logData)
				return nil, errs.ErrDatasetNotFound
			}

			state = datasetDoc.Current.State
		}

		if isStaticDataset {
			err = api.dataStore.Backend.CheckEditionExistsStatic(ctx, datasetID, edition, "")
		} else {
			err = api.dataStore.Backend.CheckEditionExists(ctx, datasetID, edition, "")
		}

		if err != nil {
			log.Error(ctx, "getMetadata endpoint: failed to find edition for dataset", err, logData)
			return nil, err
		}

		if err = models.CheckState("version", versionDoc.State); err != nil {
			logData["state"] = versionDoc.State
			log.Error(ctx, "getMetadata endpoint: unpublished version has an invalid state", err, logData)
			return nil, err
		}

		var metaDataDoc *models.Metadata

		if datasetType == models.CantabularFlexibleTable || datasetType == models.CantabularMultivariateTable {
			metaDataDoc = models.CreateCantabularMetaDataDoc(doc, versionDoc)
		} else {
			// combine version and dataset metadata
			if state != models.PublishedState {
				metaDataDoc = models.CreateMetaDataDoc(datasetDoc.Next, versionDoc, api.urlBuilder)
			} else {
				metaDataDoc = models.CreateMetaDataDoc(datasetDoc.Current, versionDoc, api.urlBuilder)
			}
		}

		if api.enableURLRewriting {
			datasetLinksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetDatasetAPIURL())
			codeListLinksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetCodeListAPIURL())

			err = utils.RewriteMetadataLinks(ctx, metaDataDoc.Links, datasetLinksBuilder)
			if err != nil {
				log.Error(ctx, "getMetadata endpoint: failed to rewrite metadata links", err, logData)
				handleMetadataErr(w, err)
				return nil, err
			}

			metaDataDoc.Dimensions, err = utils.RewriteDimensions(ctx, metaDataDoc.Dimensions, datasetLinksBuilder, codeListLinksBuilder)
			if err != nil {
				log.Error(ctx, "getMetadata endpoint: failed to rewrite metadata dimensions", err, logData)
				handleMetadataErr(w, err)
				return nil, err
			}

			err = utils.RewriteDatasetLinks(ctx, metaDataDoc.DatasetLinks, datasetLinksBuilder)
			if err != nil {
				log.Error(ctx, "getMetadata endpoint: failed to rewrite dataset links", err, logData)
				handleMetadataErr(w, err)
				return nil, err
			}

			err = utils.RewriteDownloadLinks(ctx, metaDataDoc.Downloads, api.urlBuilder.GetDownloadServiceURL())
			if err != nil {
				log.Error(ctx, "getMetadata endpoint: failed to rewrite download links", err, logData)
				handleMetadataErr(w, err)
				return nil, err
			}

			metaDataDoc.Distributions, err = utils.RewriteDistributions(ctx, metaDataDoc.Distributions, api.urlBuilder.GetDownloadServiceURL())
			if err != nil {
				log.Error(ctx, "getMetadata endpoint: failed to rewrite distributions DownloadURL", err, logData)
				handleMetadataErr(w, err)
				return nil, err
			}
		}

		b, err := json.Marshal(metaDataDoc)
		if err != nil {
			log.Error(ctx, "getMetadata endpoint: failed to marshal metadata resource into bytes", err, logData)
			return nil, err
		}
		return b, err
	}()

	if err != nil {
		log.Error(ctx, "received error", err, logData)
		handleMetadataErr(w, err)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.Error(ctx, "getMetadata endpoint: failed to write bytes to response", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Info(ctx, "getMetadata endpoint: get metadata request successful", logData)
}

func (api *DatasetAPI) putMetadata(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	versionEtag := getIfMatch(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version, "version etag": versionEtag}

	err := func() error {
		var metadata models.EditableMetadata
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			return errs.ErrUnableToReadMessage
		}

		err = json.Unmarshal(payload, &metadata)
		if err != nil {
			return errs.ErrUnableToParseJSON
		}

		versionNumber, err := models.ParseAndValidateVersionNumber(ctx, version)
		if err != nil {
			log.Error(ctx, "putMetadata endpoint: failed due to invalid version request", err, logData)
			return err
		}

		version, err := api.dataStore.Backend.GetVersion(ctx, datasetID, edition, versionNumber, "")
		if err != nil {
			if err == errs.ErrVersionNotFound {
				log.Error(ctx, "putMetadata endpoint: failed to find version for dataset edition", err, logData)
				return errs.ErrMetadataVersionNotFound
			}
			log.Error(ctx, "putMetadata endpoint: get datastore.getVersion returned an error", err, logData)
			return err
		}

		if versionEtag != mongo.AnyETag && versionEtag != version.ETag {
			logData["incomingEtag"] = versionEtag
			logData["versionEtag"] = version.ETag
			log.Error(ctx, "ETag mismatch", errs.ErrInstanceConflict, logData)
			return errs.ErrInstanceConflict
		}

		datasetDoc, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
		if err != nil {
			log.Error(ctx, "putMetadata endpoint: get datastore.getDataset returned an error", err, logData)
			return err
		}

		dataset := datasetDoc.Next
		if dataset == nil {
			err := errors.New("invalid dataset: no 'next' object found")
			log.Error(ctx, "putMetadata endpoint: failed to find next object for dataset", err, logData)
			return err
		}

		if dataset.State != models.AssociatedState || version.State != models.AssociatedState {
			err := errors.New("invalid request: can't update a record with a state other than associated")
			log.Error(ctx, "putMetadata endpoint: failed to update the record due to unexpected state", err, logData)
			return errs.ErrExpectedResourceStateOfAssociated
		}

		dataset.UpdateMetadata(metadata)
		version.UpdateMetadata(metadata)

		if err = api.dataStore.Backend.UpdateMetadata(ctx, datasetID, version.ID, versionEtag, dataset, version); err != nil {
			log.Error(ctx, "putMetadata endpoint: failed to update version resource", err, logData)
			return err
		}
		return err
	}()

	if err != nil {
		log.Error(ctx, "received error", err, logData)
		handleMetadataErr(w, err)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.Info(ctx, "putMetadata endpoint: put metadata request successful", logData)
}

func handleMetadataErr(w http.ResponseWriter, err error) {
	var responseStatus int

	switch err {
	case errs.ErrUnauthorised,
		errs.ErrEditionNotFound,
		errs.ErrMetadataVersionNotFound,
		errs.ErrDatasetNotFound:
		responseStatus = http.StatusNotFound
	case errs.ErrInvalidVersion,
		errs.ErrUnableToParseJSON,
		errs.ErrUnableToReadMessage:
		responseStatus = http.StatusBadRequest
	case errs.ErrExpectedResourceStateOfAssociated:
		responseStatus = http.StatusForbidden
	case errs.ErrInstanceConflict:
		responseStatus = http.StatusConflict
	default:
		err = errs.ErrInternalServer
		responseStatus = http.StatusInternalServerError
	}

	http.Error(w, err.Error(), responseStatus)
}

func getIfMatch(r *http.Request) string {
	ifMatch := r.Header.Get("If-Match")
	if ifMatch == "" {
		return mongo.AnyETag
	}
	return ifMatch
}
