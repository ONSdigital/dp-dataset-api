package permissions

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/ONSdigital/log.go/log"
	"github.com/pkg/errors"
)

const (
	getPermissionsURL = "http://localhost:8082/permissions?dataset_id=%s&collection_id=%s"
)

type ClientImpl struct {
	HttpClient http.Client
	Host       string
}

func (c *ClientImpl) Get(serviceToken string, userToken string, collectionID string, datasetID string) (*CallerPermissions, error) {
	url := fmt.Sprintf(getPermissionsURL, datasetID, collectionID)
	permissionReq, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	permissionReq.Header.Set("Authorization", serviceToken)
	permissionReq.Header.Set("X-Florence-Token", userToken)

	resp, err := c.HttpClient.Do(permissionReq)
	if err != nil {
		return nil, &Error{Message: "error making get permissions request", Cause: err}
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Event(nil, "error closing get permissions response body", log.Error(err))
		}
	}()

	if resp.StatusCode != 200 {
		return nil, &Error{
			Message: "get permissions request returned non 200 status",
			Cause:   errors.Errorf("expected status: 200, actual status: %d", resp.StatusCode),
		}
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, &Error{Message: "error reading get permissions response body", Cause: err}
	}

	var callerPermissions CallerPermissions
	err = json.Unmarshal(b, &callerPermissions)
	if err != nil {
		return nil, &Error{Message: "error unmarshalling get permissions response body into struct", Cause: err}
	}

	return &callerPermissions, nil
}
