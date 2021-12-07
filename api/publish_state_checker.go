package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// PublishCheck Checks if an version has been published
type PublishCheck struct {
	Datastore store.Storer
}

// Check wraps a HTTP handle. Checks that the state is not published
func (d *PublishCheck) Check(handle func(http.ResponseWriter, *http.Request), action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		ctx := r.Context()
		vars := mux.Vars(r)
		datasetID := vars["dataset_id"]
		edition := vars["edition"]
		version := vars["version"]
		data := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}
		versionId, err := models.ParseAndValidateVersionNumber(ctx, version)
		if err != nil {
			log.Error(ctx, "failed due to invalid version request", err, data)
			dphttp.DrainBody(r)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		currentVersion, err := d.Datastore.GetVersion(ctx, datasetID, edition, versionId, "")
		if err != nil {
			if err != errs.ErrVersionNotFound {
				log.Error(ctx, "errored whilst retrieving version resource", err, data)
				dphttp.DrainBody(r)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			// If document cannot be found do not handle error
			handle(w, r)
			return
		}

		if currentVersion != nil {
			if currentVersion.State == models.PublishedState {

				// We can allow public download links to be modified by the exporter
				// services when a version is published. Note that a new version will be
				// created which contain only the download information to prevent any
				// forbidden fields from being set on the published version

				// TODO Logic here might require it's own endpoint,
				// possibly /datasets/.../versions/<version>/downloads
				if action == updateVersionAction {
					versionDoc, err := models.CreateVersion(r.Body, datasetID)
					if err != nil {
						log.Error(ctx, "failed to model version resource based on request", err, data)
						dphttp.DrainBody(r)
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}

					if versionDoc.Downloads != nil {
						newVersion := &models.Version{Downloads: &models.DownloadList{}}
						if versionDoc.Downloads.CSV != nil && versionDoc.Downloads.CSV.Public != "" {
							newVersion.Downloads.CSV = &models.DownloadObject{
								Public: versionDoc.Downloads.CSV.Public,
								Size:   versionDoc.Downloads.CSV.Size,
								HRef:   versionDoc.Downloads.CSV.HRef,
							}
						}

						if versionDoc.Downloads.CSVW != nil && versionDoc.Downloads.CSVW.Public != "" {
							newVersion.Downloads.CSVW = &models.DownloadObject{
								Public: versionDoc.Downloads.CSVW.Public,
								Size:   versionDoc.Downloads.CSVW.Size,
								HRef:   versionDoc.Downloads.CSVW.HRef,
							}
						}

						if versionDoc.Downloads.XLS != nil && versionDoc.Downloads.XLS.Public != "" {
							newVersion.Downloads.XLS = &models.DownloadObject{
								Public: versionDoc.Downloads.XLS.Public,
								Size:   versionDoc.Downloads.XLS.Size,
								HRef:   versionDoc.Downloads.XLS.HRef,
							}
						}

						if versionDoc.Downloads.TXT != nil && versionDoc.Downloads.TXT.Public != "" {
							newVersion.Downloads.TXT = &models.DownloadObject{
								Public: versionDoc.Downloads.TXT.Public,
								Size:   versionDoc.Downloads.TXT.Size,
								HRef:   versionDoc.Downloads.TXT.HRef,
							}
						}

						if newVersion != nil {
							var b []byte
							b, err = json.Marshal(newVersion)
							if err != nil {
								log.Error(ctx, "failed to marshal new version resource based on request", err, data)
								dphttp.DrainBody(r)
								http.Error(w, err.Error(), http.StatusForbidden)
								return
							}

							if err = r.Body.Close(); err != nil {
								log.Error(ctx, "could not close response body", err, data)
							}

							// Set variable `has_downloads` to true to prevent request
							// triggering version from being republished
							vars[hasDownloads] = trueStringified
							r.Body = ioutil.NopCloser(bytes.NewBuffer(b))
							handle(w, r)
							return
						}
					}
				}

				err = errors.New("unable to update version as it has been published")
				data["version"] = currentVersion
				log.Error(ctx, "failed to update version", err, data)
				dphttp.DrainBody(r)
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
		}

		handle(w, r)
	}
}
