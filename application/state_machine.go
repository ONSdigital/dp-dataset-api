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
	transitions map[State]Transition
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

	fmt.Println("The new state is")
	fmt.Println(newstate)
	for state, trasitions := range sm.transitions {
		fmt.Println(state)
		fmt.Println(trasitions)
		if state == newstate {
			fmt.Println("The states match")
			for i := 0; i < len(trasitions.AlllowedSourceStates); i++ {
				fmt.Println(trasitions.AlllowedSourceStates[i])
				fmt.Println(previousState)
				if previousState == trasitions.AlllowedSourceStates[i] {
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

func NewStateMachine(states map[string]State, transitions map[State]Transition, dataStore store.DataStore, ctx context.Context) *StateMachine {
	sm := &StateMachine{
		states:      states,
		transitions: transitions,
		dataStore:   dataStore,
		ctx:         ctx,
	}

	return sm
}
