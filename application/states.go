package application

import (
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/models"
)

type Created struct{}

func (g Created) Enter(combinedVersionUpdate *models.Version, l *StateMachine) error {
	fmt.Println("Creating")
	return nil
}
func (g Created) String() string {
	return "created"
}

type Submitted struct{}

func (g Submitted) Enter(combinedVersionUpdate *models.Version, l *StateMachine) error {
	fmt.Println("Submitting")
	return nil
}
func (g Submitted) String() string {
	return "submitted"
}

type Completed struct{}

func (g Completed) Enter(combinedVersionUpdate *models.Version, l *StateMachine) error {
	fmt.Println("completing")
	return nil
}
func (g Completed) String() string {
	return "completed"
}

type Failed struct{}

func (g Failed) Enter(combinedVersionUpdate *models.Version, l *StateMachine) error {
	fmt.Println("Failing")
	return nil
}
func (g Failed) String() string {
	return "failed"
}

type EditionConfirmed struct{}

func (g EditionConfirmed) Enter(combinedVersionUpdate *models.Version, l *StateMachine) error {
	fmt.Println("Edition confirming")
	return nil
}
func (g EditionConfirmed) String() string {
	return "edition-confirmed"
}

type Detached struct{}

func (g Detached) Enter(combinedVersionUpdate *models.Version, l *StateMachine) error {
	fmt.Println("Detaching")
	return nil
}
func (g Detached) String() string {
	return "detached"
}

type Associated struct{}

func (g Associated) Enter(combinedVersionUpdate *models.Version, l *StateMachine) error {
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

type Published struct{}

func (g Published) Enter(combinedVersionUpdate *models.Version, l *StateMachine) error {
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
