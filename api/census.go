package api

import "net/http"

func (api *DatasetAPI) getCensus(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Write([]byte("{}"))
}
