package state

import (
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/models"
)

type State interface {
	Enter(combinedVersionUpdate *models.Version)
	String() string
	Update(l *StateMachine)
}

type Created struct{}

func (g Created) Enter(combinedVersionUpdate *models.Version) {
	fmt.Println("Creating")
}
func (g Created) String() string {
	return "created"
}
func (g Created) Update(l *StateMachine) {
	l.setState(&Created{})
}

type Submitted struct{}

func (g Submitted) Enter(combinedVersionUpdate *models.Version) {
	fmt.Println("Submitting")
}
func (g Submitted) String() string {
	return "submitted"
}
func (g Submitted) Update(l *StateMachine) {
	l.setState(Submitted{})
}

type Completed struct{}

func (g Completed) Enter(combinedVersionUpdate *models.Version) {
	fmt.Println("completing")
}
func (g Completed) String() string {
	return "completed"
}
func (g Completed) Update(l *StateMachine) {
	l.setState(&Completed{})
}

type Failed struct{}

func (g Failed) Enter(combinedVersionUpdate *models.Version) {
	fmt.Println("Failing")
}
func (g Failed) String() string {
	return "failed"
}
func (g Failed) Update(l *StateMachine) {
	l.setState(&Failed{})
}

type EditionConfirmed struct{}

func (g EditionConfirmed) Enter(combinedVersionUpdate *models.Version) {
	fmt.Println("Edition confirming")
}
func (g EditionConfirmed) String() string {
	return "edition-confirmed"
}
func (g EditionConfirmed) Update(l *StateMachine) {
	l.setState(&EditionConfirmed{})
}

type Detached struct{}

func (g Detached) Enter(combinedVersionUpdate *models.Version) {
	fmt.Println("Detaching")
}
func (g Detached) String() string {
	return "detached"
}
func (g Detached) Update(l *StateMachine) {
	l.setState(&Detached{})
}

type Associated struct{}

func (g Associated) Enter(combinedVersionUpdate *models.Version) {
	fmt.Println("Associating")
}
func (g Associated) String() string {
	return "associated"
}
func (g Associated) Update(l *StateMachine) {
	l.setState(&Associated{})
}

type Published struct{}

func (g Published) Enter(combinedVersionUpdate *models.Version) {
	fmt.Println("Publishing")
	// This needs to do the validation on required fields etc.
	err := models.ValidateVersion(combinedVersionUpdate)
	if err != nil {
		fmt.Println("Validation Failed")
		fmt.Println(err)
	} else {
		fmt.Println("Validation passed, continue")
	}
}
func (g Published) String() string {
	return "published"
}
func (g Published) Update(l *StateMachine) {
	l.setState(&Published{})
}
