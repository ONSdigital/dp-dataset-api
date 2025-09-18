package application

import (
	"context"
	"errors"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/log.go/v2/log"
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
	transitions map[KeyVal][]string
	DataStore   store.DataStore
	ctx         context.Context
}

type Transition struct {
	Label               string
	TargetState         State
	AllowedSourceStates []string
	Type                string
}

type KeyVal struct {
	StateVal string
	Type     string
}

func castStateToState(state string) (*State, bool) {
	switch s := state; s {
	case "published":
		return &Published, true
	case "associated":
		return &Associated, true
	case "approved":
		return &Approved, true
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
		if currentVersion.Type == "" {
			currentVersion.Type = "v4"
		}

		if state.StateVal == versionUpdate.State && state.Type == currentVersion.Type {
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
		if currentVersion.State == versionUpdate.State && versionUpdate.State == "published" {
			log.Info(ctx, "state machine: version already published, treating as successful")
			return nil
		}
		return errors.New("state not allowed to transition")
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

	transitionsMap := make(map[KeyVal][]string)
	for _, transition := range transitions {
		transitionsMap[KeyVal{StateVal: transition.TargetState.String(), Type: transition.Type}] = transition.AllowedSourceStates
	}

	sm := &StateMachine{
		states:      statesMap,
		transitions: transitionsMap,
		DataStore:   dataStore,
		ctx:         ctx,
	}

	return sm
}
