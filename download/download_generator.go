package download

import (
	"fmt"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/clients/filter"
	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"
	"strconv"
	"time"
)

//go:generate moq -out mocks/generate_downloads_mocks.go -pkg mocks . FilterClient Store

var (
	datasetIDKey            = "datasetID"
	editionKey              = "edition"
	versionKey              = "version"
	downloadsKey            = "downloads"
	xlsKey                  = "xls"
	csvKey                  = "csv"
	maxRetires              = 5
	createBlueprintErr      = "error while attempting to create filter blueprint: url: %s"
	getJobStateErr          = "error while attempting to get filter job state filterID: %s, url: %s"
	updateBlueprintErr      = "error while attempting to update filter blueprint filterID: %s, url: %s"
	retriesExceededErr      = "dataset downloads not available and check retries exceeded max retry attempts: url: %s"
	getOutputErr            = "filter client get output returned an error filterID: %s"
	getVersionErr           = "error while attempting to get latest dataset version: url: %s"
	updateDatasetVersionErr = "error while attempting to updated dataset version"
	datasetVersionURIFMT    = "/datasets/%s/editions/%s/versions/%d"

	datasetIDEmptyErr       = newGeneratorError(nil, "failed to generator full datasetID download as dataset was empty")
	editionEmptyErr         = newGeneratorError(nil, "failed to generator full datasetID download as edition was empty")
	versionIDEmptyErr       = newGeneratorError(nil, "failed to generator full datasetID download as versionID was empty")
	versionNumberInvalidErr = newGeneratorError(nil, "failed to generator full datasetID download as version was invalid")
)

type FilterClient interface {
	// CreateBlueprint ...
	CreateBlueprint(instanceID string, names []string) (string, error)
	// GetJobState ...
	GetJobState(filterID string) (m filter.Model, err error)
	// UpdateBlueprint ...
	UpdateBlueprint(m filter.Model, doSubmit bool) (mdl filter.Model, err error)
	// GetOutput ...
	GetOutput(filterOutputID string) (m filter.Model, err error)
}

type Store interface {
	GetVersion(datasetID, editionID, version, state string) (*models.Version, error)
	UpdateVersion(ID string, version *models.Version) error
}

type Generator struct {
	FilterClient FilterClient
	Store        Store
	Delay        time.Duration
	MaxRetries   int
}

type GeneratorError struct {
	originalErr error
	message     string
	args        []interface{}
}

func newGeneratorError(err error, message string, args ...interface{}) GeneratorError {
	return GeneratorError{
		originalErr: err,
		message:     message,
		args:        args,
	}
}

func (genErr GeneratorError) Error() string {
	if genErr.originalErr == nil {
		return errors.Errorf(genErr.message, genErr.args...).Error()
	}
	return errors.Wrap(genErr.originalErr, fmt.Sprintf(genErr.message, genErr.args...)).Error()
}

func (g Generator) GenerateDatasetDownloads(datasetID string, edition string, versionID string, version int) error {
	if err := g.validate(datasetID, edition, versionID, version); err != nil {
		return err
	}

	versionURL := versionURI(datasetID, edition, version)
	filterID, err := g.FilterClient.CreateBlueprint(versionID, []string{})
	if err != nil {
		return newGeneratorError(err, createBlueprintErr, versionURL)
	}

	jobState, err := g.FilterClient.GetJobState(filterID)
	if err != nil {
		return newGeneratorError(err, getJobStateErr, filterID, versionURL)
	}

	updatedBlueprint, err := g.FilterClient.UpdateBlueprint(jobState, true)
	if err != nil {
		return newGeneratorError(err, updateBlueprintErr, jobState.FilterID, versionURL)
	}

	filterOutput, err := g.checkDownloadsAvailable(updatedBlueprint.Links.FilterOutputs.ID, versionURL)
	if err != nil {
		return err
	}

	log.Info("full dataset version downloads available", log.Data{
		downloadsKey: filterOutput.Downloads,
		datasetIDKey: datasetID,
		editionKey:   edition,
		versionKey:   version,
	})

	versionStr := strconv.Itoa(version)
	if versionStr == "0" {
		versionStr = "1"
	}

	latestVersion, err := g.Store.GetVersion(datasetID, edition, versionStr, models.AssociatedState)
	if err != nil {
		return newGeneratorError(err, getVersionErr, versionURL)
	}

	xls := filterOutput.Downloads[xlsKey]
	csv := filterOutput.Downloads[csvKey]

	latestVersion.Downloads = &models.DownloadList{
		XLS: &models.DownloadObject{URL: xls.URL, Size: xls.Size},
		CSV: &models.DownloadObject{URL: csv.URL, Size: csv.Size},
	}

	if err := g.Store.UpdateVersion(latestVersion.ID, latestVersion); err != nil {
		return newGeneratorError(err, updateDatasetVersionErr, versionURL)
	}
	return nil
}

func (g Generator) checkDownloadsAvailable(outputID string, versionURL string) (*filter.Model, error) {
	var filterOutput filter.Model
	var err error
	available := false

	for retries := 0; retries < maxRetires; retries++ {
		filterOutput, err = g.FilterClient.GetOutput(outputID)
		if err != nil {
			log.Error(newGeneratorError(err, getOutputErr, outputID), nil)
			continue
		}
		if g.downloadsAvailable(filterOutput) {
			break
		}
		time.Sleep(time.Second * g.Delay)
	}

	if !available {
		return nil, errors.Errorf(retriesExceededErr, versionURL)
	}
	return &filterOutput, nil
}

func (g Generator) validate(datasetID string, edition string, versionID string, version int) error {
	if datasetID == "" {
		return datasetIDEmptyErr
	}
	if edition == "" {
		return editionEmptyErr
	}
	if versionID == "" {
		return versionIDEmptyErr
	}
	if version == 0 {
		return versionNumberInvalidErr
	}
	return nil
}

func versionURI(datasetID string, edition string, version int) string {
	return fmt.Sprintf(datasetVersionURIFMT, datasetID, edition, version)
}

func (g Generator) downloadsAvailable(f filter.Model) bool {
	var xlsDownload filter.Download
	var csvDownload filter.Download
	var ok bool

	if f.Downloads == nil || len(f.Downloads) == 0 {
		return false
	}
	if xlsDownload, ok = f.Downloads[xlsKey]; !ok {
		return false
	}
	if csvDownload, ok = f.Downloads[csvKey]; !ok {
		return false
	}
	if xlsDownload.URL == "" || csvDownload.URL == "" {
		return false
	}
	return true
}
