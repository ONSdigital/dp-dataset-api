package state

import (
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

func (sm *StateMachine) setState(s State) {
	fmt.Println("Entering setstate")
	sm.newState = s
	sm.newState.Enter(sm.combinedVersionUpdate)
}

func (sm *StateMachine) Transition() {
	if _, ok := sm.states[sm.existingState]; ok {
		fmt.Println("Previous state is allowed it's ok")
		sm.newState.Update(sm)
	} else {
		fmt.Println("State not allowed to transition")
		fmt.Println(" cannot move from " + sm.existingState + "  to ")
		fmt.Println(sm.newState)
	}

	fmt.Println("Exiting transition")
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
