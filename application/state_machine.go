package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
)

type State interface {
	Enter(combinedVersionUpdate *models.Version, l *StateMachine, smDS *StateMachineDatasetAPI, ctx context.Context,
		currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
		currentVersion *models.Version, // Called Instances in Mongo
		versionUpdate *models.Version, // Next version, that is the new version
		versionDetails VersionDetails) error
	String() string
}

type StateMachine struct {
	states      map[string]State
	transitions map[string][]string
	dataStore   store.DataStore
	ctx         context.Context
}

type Transition struct {
	Label                string
	TargetState          State
	AlllowedSourceStates []string
}

func castStateToState(state string) State {

	switch s := state; s {
	case "published":
		return Published{}
	case "associated":
		return Associated{}
	case "created":
		return Created{}
	case "completed":
		return Completed{}
	case "edition-confirmed":
		return EditionConfirmed{}
	case "detached":
		return Detached{}
	case "submitted":
		return Submitted{}
	case "failed":
		return Failed{}
	default:
		return nil
	}
}

func (sm *StateMachine) Transition(combinedVersionUpdate *models.Version, newState string, previousState string, smDS *StateMachineDatasetAPI, ctx context.Context,
	currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails) error {

	match := false
	var nextState State

	for state, transitions := range sm.transitions {
		fmt.Println(state)
		fmt.Println(transitions)

		if state == newState {
			fmt.Println("The states match")
			for i := 0; i < len(transitions); i++ {
				if previousState == transitions[i] {
					match = true
					nextState = castStateToState(newState)
				}
			}
		}

	}

	if !match {
		fmt.Println("State not allowed to transition")
		return errors.New("invalid state")
	}

	fmt.Println("Previous state is allowed it's ok")
	err := nextState.Enter(combinedVersionUpdate, sm, smDS, ctx,
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

	fmt.Println("The transitions map is")
	fmt.Println(transitionsMap)
	sm := &StateMachine{
		states:      statesMap,
		transitions: transitionsMap,
		dataStore:   dataStore,
		ctx:         ctx,
	}

	return sm
}
