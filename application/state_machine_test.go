package application

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCastStateToState(t *testing.T) {
	t.Parallel()
	Convey("When a string is converted to a state", t, func() {
		publishedState, publishedOk := castStateToState("published")
		So(publishedState.Name, ShouldEqual, Published.Name)
		So(publishedOk, ShouldBeTrue)

		associatedState, associatedOk := castStateToState("associated")
		So(associatedState.Name, ShouldEqual, Associated.Name)
		So(associatedOk, ShouldBeTrue)

		editionConfirmedState, editionConfirmedOk := castStateToState("edition-confirmed")
		So(editionConfirmedState.Name, ShouldEqual, EditionConfirmed.Name)
		So(editionConfirmedOk, ShouldBeTrue)

		createdState, createdOk := castStateToState("created")
		So(createdState.Name, ShouldEqual, Created.Name)
		So(createdOk, ShouldBeTrue)

		completedState, completedOk := castStateToState("completed")
		So(completedState.Name, ShouldEqual, Completed.Name)
		So(completedOk, ShouldBeTrue)

		detachedState, detachedOk := castStateToState("detached")
		So(detachedState.Name, ShouldEqual, Detached.Name)
		So(detachedOk, ShouldBeTrue)

		failedState, failedOk := castStateToState("failed")
		So(failedState.Name, ShouldEqual, Failed.Name)
		So(failedOk, ShouldBeTrue)

		submittedState, submittedOk := castStateToState("submitted")
		So(submittedState.Name, ShouldEqual, Submitted.Name)
		So(submittedOk, ShouldBeTrue)

		nilState, ok := castStateToState("")
		So(nilState, ShouldBeNil)
		So(ok, ShouldBeFalse)
	})
}

func TestTransition(t *testing.T) {

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	states, transitions := setUpStatesTransitions()

	mockedDataStore := &storetest.StorerMock{
		UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
			return "", nil
		},
	}

	stateMachine := NewStateMachine(states, transitions, store.DataStore{Backend: mockedDataStore}, testContext)
	smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

	Convey("The transition is successful", t, func() {

		err := smDS.StateMachine.Transition(smDS, testContext, currentDataset, currentVersionEditionConfirmed, versionUpdateAssociated, versionDetails, "true")

		So(err, ShouldBeNil)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)

	})

	Convey("The transition is not successful", t, func() {

		incorrectStateVersion := &models.Version{
			State:        "not_a_state",
			ReleaseDate:  "2024-12-31",
			Version:      1,
			ID:           "789",
			CollectionID: "3434",
		}

		currentIncorrectState := &models.Version{
			State:        "not_a_state",
			ReleaseDate:  "2024-12-31",
			Version:      1,
			ID:           "789",
			CollectionID: "3434",
		}

		err := smDS.StateMachine.Transition(smDS, testContext, currentDataset, currentIncorrectState, incorrectStateVersion, versionDetails, "true")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "State not allowed to transition")

	})
}
