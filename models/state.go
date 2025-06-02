package models

import (
	"fmt"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
)

// A list of reusable states across application
const (
	CreatedState          = "created"
	SubmittedState        = "submitted"
	CompletedState        = "completed"
	EditionConfirmedState = "edition-confirmed"
	AssociatedState       = "associated"
	PublishedState        = "published"
	DetachedState         = "detached"
	FailedState           = "failed"
)

var validVersionStates = map[string]int{
	EditionConfirmedState: 1,
	AssociatedState:       1,
	PublishedState:        1,
}

var validStates = map[string]int{
	CreatedState:          1,
	SubmittedState:        1,
	CompletedState:        1,
	EditionConfirmedState: 1,
	AssociatedState:       1,
	PublishedState:        1,
	FailedState:           1,
}

// ValidateStateFilter checks the list of filter states from a whitelist
func ValidateStateFilter(filterList []string) error {
	var invalidFilterStateValues []string

	for _, filter := range filterList {
		if _, ok := validStates[filter]; !ok {
			invalidFilterStateValues = append(invalidFilterStateValues, filter)
		}
	}

	if invalidFilterStateValues != nil {
		err := fmt.Errorf("bad request - invalid filter state values: %v", invalidFilterStateValues)
		return err
	}

	return nil
}

// ValidateInstanceState checks the list of instance states from a whitelist
func ValidateInstanceState(state string) error {
	if _, ok := validStates[state]; !ok {
		return fmt.Errorf("bad request - invalid instance state: %s", state)
	}

	return nil
}

// CheckState checks state against a whitelist of valid states
func CheckState(docType, state string) error {
	var states map[string]int
	switch docType {
	case "version":
		states = validVersionStates
	default:
		states = validStates
	}

	if states[state] == 1 {
		return nil
	}

	return errs.ErrResourceState
}

type StateUpdate struct {
	State string `json:"state"`
}
