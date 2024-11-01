package application

import (
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/models"
)

type State interface {
	Enter(combinedVersionUpdate *models.Version) error
	String() string
	Update(l *StateMachine) error
}

type Created struct{}

func (g Created) Enter(combinedVersionUpdate *models.Version) error {
	fmt.Println("Creating")
	return nil
}
func (g Created) String() string {
	return "created"
}
func (g Created) Update(l *StateMachine) error {
	err := l.setState(&Created{})
	if err != nil {
		return err
	}
	return nil
}

type Submitted struct{}

func (g Submitted) Enter(combinedVersionUpdate *models.Version) error {
	fmt.Println("Submitting")
	return nil
}
func (g Submitted) String() string {
	return "submitted"
}
func (g Submitted) Update(l *StateMachine) error {
	err := l.setState(Submitted{})
	if err != nil {
		return err
	}
	return nil
}

type Completed struct{}

func (g Completed) Enter(combinedVersionUpdate *models.Version) error {
	fmt.Println("completing")
	return nil
}
func (g Completed) String() string {
	return "completed"
}
func (g Completed) Update(l *StateMachine) error {
	err := l.setState(&Completed{})
	if err != nil {
		return err
	}
	return nil
}

type Failed struct{}

func (g Failed) Enter(combinedVersionUpdate *models.Version) error {
	fmt.Println("Failing")
	return nil
}
func (g Failed) String() string {
	return "failed"
}
func (g Failed) Update(l *StateMachine) error {
	err := l.setState(&Failed{})
	if err != nil {
		return err
	}
	return nil
}

type EditionConfirmed struct{}

func (g EditionConfirmed) Enter(combinedVersionUpdate *models.Version) error {
	fmt.Println("Edition confirming")
	return nil
}
func (g EditionConfirmed) String() string {
	return "edition-confirmed"
}
func (g EditionConfirmed) Update(l *StateMachine) error {
	err := l.setState(&EditionConfirmed{})
	if err != nil {
		return err
	}
	return nil
}

type Detached struct{}

func (g Detached) Enter(combinedVersionUpdate *models.Version) error {
	fmt.Println("Detaching")
	return nil
}
func (g Detached) String() string {
	return "detached"
}
func (g Detached) Update(l *StateMachine) error {
	err := l.setState(&Detached{})
	if err != nil {
		return err
	}
	return nil
}

type Associated struct{}

func (g Associated) Enter(combinedVersionUpdate *models.Version) error {
	fmt.Println("Associating")
	err := models.ValidateVersion(combinedVersionUpdate)
	if err != nil {
		fmt.Println("Validation Failed")
		fmt.Println(err)
		return err
	} else {
		fmt.Println("Validation passed, continue")
		return nil
	}
}
func (g Associated) String() string {
	return "associated"
}
func (g Associated) Update(l *StateMachine) error {

	err := l.setState(&Associated{})
	if err != nil {
		return err
	}
	return nil
}

type Published struct{}

func (g Published) Enter(combinedVersionUpdate *models.Version) error {
	fmt.Println("Publishing")
	// This needs to do the validation on required fields etc.
	err := models.ValidateVersion(combinedVersionUpdate)
	if err != nil {
		fmt.Println("Validation Failed")
		fmt.Println(err)
		return err
	} else {
		fmt.Println("Validation passed, continue")
		return nil
	}
}
func (g Published) String() string {
	return "published"
}
func (g Published) Update(l *StateMachine) error {
	err := l.setState(&Published{})
	if err != nil {
		return err
	}
	return nil
}
