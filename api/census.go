package api

import "net/http"

func (api *DatasetAPI) getCensus(writer http.ResponseWriter, request *http.Request) {
	writer.Write([]byte{0, 1, 2})
}
