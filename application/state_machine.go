package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
)

type State interface {
	Enter(combinedVersionUpdate *models.Version, l *StateMachine) error
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

func (sm *StateMachine) Transition(combinedVersionUpdate *models.Version, newstate State, previousState string) error {

	match := false

	for state, transitions := range sm.transitions {
		fmt.Println(state)
		fmt.Println(transitions)

		if state == newstate.String() {
			fmt.Println("The states match")
			for i := 0; i < len(transitions); i++ {
				if previousState == transitions[i] {
					match = true
				}
			}
		}

	}

	if !match {
		fmt.Println("State not allowed to transition")
		return errors.New("invalid state")
	}

	fmt.Println("Previous state is allowed it's ok")
	err := newstate.Enter(combinedVersionUpdate, sm)
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
