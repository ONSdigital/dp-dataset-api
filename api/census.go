package api

import (
	"encoding/json"
	"net/http"
)

func (api *DatasetAPI) getCensus(writer http.ResponseWriter, request *http.Request) {
	blobs, err := api.dataStore.Backend.Blobs(request.Context())
	if err != nil {
		panic("not implemented")
	}

	serializedBlobs, err := json.Marshal(blobs)
	if err != nil {
		panic("not implemented")
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(serializedBlobs)
}
