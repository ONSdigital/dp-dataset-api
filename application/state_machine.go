package application

import (
	"context"
	"errors"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
)

type State struct {
	Name      string
	EnterFunc func(ctx context.Context, smDS *StateMachineDatasetAPI,
		currentVersion *models.Version, // Called Instances in Mongo
		versionUpdate *models.Version, // Next version, that is the new version
		versionDetails VersionDetails,
		hasDownloads string) error
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
	case "edition-confirmed":
		return &EditionConfirmed, true
	default:
		return nil, false
	}
}

func (sm *StateMachine) Transition(ctx context.Context, smDS *StateMachineDatasetAPI,
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails,
	hasDownloads string) error {
	match := false
	var nextState *State
	var ok bool

	for state, transitions := range sm.transitions {
		if state == versionUpdate.State {
			for i := 0; i < len(transitions); i++ {
				if currentVersion.State == transitions[i] {
					match = true
					nextState, ok = castStateToState(versionUpdate.State)
					if !ok {
						return errors.New("incorrect state value")
					}
					break
				}
			}
		}
	}

	if !match {
		return errors.New("State not allowed to transition")
	}

	err := nextState.EnterFunc(ctx, smDS,
		currentVersion, // Called Instances in Mongo
		versionUpdate,  // Next version, that is the new version
		versionDetails,
		hasDownloads)
	if err != nil {
		return err
	}
	return nil
}

func NewStateMachine(ctx context.Context, states []State, transitions []Transition, dataStore store.DataStore) *StateMachine {
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
