package application

import (
	"errors"
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/models"
)

type StateMachine struct {
	existingState         string
	newState              State
	states                map[string]State
	event                 string
	combinedVersionUpdate *models.Version
}

func (sm *StateMachine) setState(s State) error {
	fmt.Println("Entering setstate")
	sm.newState = s
	err := sm.newState.Enter(sm.combinedVersionUpdate)
	if err != nil {
		fmt.Println("the enter function returned an error")
		return err
	}

	return nil
}

func (sm *StateMachine) Transition() error {
	if _, ok := sm.states[sm.existingState]; ok {
		fmt.Println("Previous state is allowed it's ok")
		err := sm.newState.Update(sm)
		if err != nil {
			return err
		}
		return nil
	} else {
		fmt.Println("State not allowed to transition")
		fmt.Println(" cannot move from " + sm.existingState + "  to ")
		fmt.Println(sm.newState)

		return errors.New("invalid state")
	}
}

func NewStateMachine(existingState string, newState State, stateList map[string]State, combinedVersionUpdate *models.Version) *StateMachine {
	sm := &StateMachine{
		existingState:         existingState,
		newState:              newState,
		states:                stateList,
		combinedVersionUpdate: combinedVersionUpdate,
	}

	sm.newState.Enter(combinedVersionUpdate)
	return sm
}
