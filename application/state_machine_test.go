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
		So(associatedState.Name, ShouldEqual, associatedState.Name)
		So(associatedOk, ShouldBeTrue)

		editionConfirmedState, editionConfirmedOk := castStateToState("edition-confirmed")
		So(editionConfirmedState.Name, ShouldEqual, EditionConfirmed.Name)
		So(editionConfirmedOk, ShouldBeTrue)

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

	stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})
	smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

	Convey("The transition is successful", t, func() {
		err := smDS.StateMachine.Transition(testContext, smDS, currentVersionEditionConfirmed, versionUpdateAssociated, versionDetails, "true")

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

		err := smDS.StateMachine.Transition(testContext, smDS, currentIncorrectState, incorrectStateVersion, versionDetails, "true")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "state not allowed to transition")
	})
}
