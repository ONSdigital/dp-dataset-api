package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
)

type State struct {
	Name      string
	EnterFunc func(smDS *StateMachineDatasetAPI, ctx context.Context,
		currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
		currentVersion *models.Version, // Called Instances in Mongo
		versionUpdate *models.Version, // Next version, that is the new version
		versionDetails VersionDetails) error
}

func (s State) String() string {
	return s.Name
}

type StateMachine struct {
	states      map[string]State
	transitions map[string][]string
	DataStore   store.DataStore
	ctx         context.Context
}

type Transition struct {
	Label                string
	TargetState          State
	AlllowedSourceStates []string
}

func castStateToState(state string) (*State, bool) {

	switch s := state; s {
	case "published":
		return &Published, true
	case "associated":
		return &Associated, true
	case "created":
		return &Created, true
	case "completed":
		return &Completed, true
	case "edition-confirmed":
		return &EditionConfirmed, true
	case "detached":
		return &Detached, true
	case "submitted":
		return &Submitted, true
	case "failed":
		return &Failed, true
	default:
		return nil, false
	}
}

func (sm *StateMachine) Transition(smDS *StateMachineDatasetAPI, ctx context.Context,
	currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails) error {

	fmt.Println("DOING SM TRANSITION")
	fmt.Println("THE CURRENT STATE IS")
	fmt.Println(currentVersion.State)
	fmt.Println("THE UPDATED STATE IS")
	fmt.Println(versionUpdate.State)
	match := false
	var nextState *State
	var ok bool

	for state, transitions := range sm.transitions {

		if state == versionUpdate.State {
			for i := 0; i < len(transitions); i++ {
				if currentVersion.State == transitions[i] {
					match = true
					nextState, ok = castStateToState(versionUpdate.State)
					// Could the type be added in here?
					// if state == Created.Name && nextState == &Published && versionUpdate.Type == "v4" {
					// 	ok = false
					// }
					if !ok {
						return errors.New("incorrect state value")
					}
					break
				}
			}
		}

	}

	if !match {
		fmt.Println("State not allowed to transition")
		return errors.New("invalid state")
	}

	err := nextState.EnterFunc(smDS, ctx,
		currentDataset, // Called Dataset in Mongo
		currentVersion, // Called Instances in Mongo
		versionUpdate,  // Next version, that is the new version
		versionDetails)
	if err != nil {
		return err
	}
	return nil

}

func NewStateMachine(states []State, transitions []Transition, dataStore store.DataStore, ctx context.Context) *StateMachine {

	statesMap := make(map[string]State)
	for _, state := range states {
		statesMap[state.String()] = state
	}

	transitionsMap := make(map[string][]string)
	for _, transition := range transitions {
		transitionsMap[transition.TargetState.String()] = transition.AlllowedSourceStates
	}

	sm := &StateMachine{
		states:      statesMap,
		transitions: transitionsMap,
		DataStore:   dataStore,
		ctx:         ctx,
	}

	return sm
}
