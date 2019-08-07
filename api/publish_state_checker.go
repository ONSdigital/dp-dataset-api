package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/request"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// PublishCheck Checks if an version has been published
type PublishCheck struct {
	Datastore store.Storer
	Auditor   audit.AuditorService
}

// Check wraps a HTTP handle. Checks that the state is not published
func (d *PublishCheck) Check(handle func(http.ResponseWriter, *http.Request), action string) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		ctx := r.Context()
		vars := mux.Vars(r)
		datasetID := vars["dataset_id"]
		edition := vars["edition"]
		version := vars["version"]
		data := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}
		auditParams := common.Params{"dataset_id": datasetID, "edition": edition, "version": version}

		currentVersion, err := d.Datastore.GetVersion(datasetID, edition, version, "")
		if err != nil {
			if err != errs.ErrVersionNotFound {
				log.ErrorCtx(ctx, errors.WithMessage(err, "errored whilst retrieving version resource"), data)

				if auditErr := d.Auditor.Record(ctx, action, audit.Unsuccessful, auditParams); auditErr != nil {
					err = errs.ErrInternalServer
				}

				request.DrainBody(r)
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
					versionDoc, err := models.CreateVersion(r.Body)
					if err != nil {
						log.ErrorCtx(ctx, errors.WithMessage(err, "failed to model version resource based on request"), data)

						if auditErr := d.Auditor.Record(ctx, action, audit.Unsuccessful, auditParams); auditErr != nil {
							request.DrainBody(r)
							http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
							return
						}

						request.DrainBody(r)
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

						if newVersion != nil {
							var b []byte
							b, err = json.Marshal(newVersion)
							if err != nil {
								log.ErrorCtx(ctx, errors.WithMessage(err, "failed to marshal new version resource based on request"), data)

								if auditErr := d.Auditor.Record(ctx, action, audit.Unsuccessful, auditParams); auditErr != nil {
									request.DrainBody(r)
									http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
									return
								}

								request.DrainBody(r)
								http.Error(w, err.Error(), http.StatusForbidden)
								return
							}

							if err = r.Body.Close(); err != nil {
								log.ErrorCtx(ctx, errors.WithMessage(err, "could not close response body"), data)
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
				log.ErrorCtx(ctx, err, data)
				if auditErr := d.Auditor.Record(ctx, action, audit.Unsuccessful, auditParams); auditErr != nil {
					request.DrainBody(r)
					http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
					return
				}

				request.DrainBody(r)
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
		}

		handle(w, r)
	})
}
