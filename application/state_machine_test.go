package application

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/smartystreets/goconvey/convey"
)

func TestCastStateToState(t *testing.T) {
	t.Parallel()
	convey.Convey("When a string is converted to a state", t, func() {
		publishedState, publishedOk := castStateToState("published")
		convey.So(publishedState.Name, convey.ShouldEqual, Published.Name)
		convey.So(publishedOk, convey.ShouldBeTrue)

		associatedState, associatedOk := castStateToState("associated")
		convey.So(associatedState.Name, convey.ShouldEqual, associatedState.Name)
		convey.So(associatedOk, convey.ShouldBeTrue)

		editionConfirmedState, editionConfirmedOk := castStateToState("edition-confirmed")
		convey.So(editionConfirmedState.Name, convey.ShouldEqual, EditionConfirmed.Name)
		convey.So(editionConfirmedOk, convey.ShouldBeTrue)

		nilState, ok := castStateToState("")
		convey.So(nilState, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
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

	convey.Convey("The transition is successful", t, func() {
		err := smDS.StateMachine.Transition(testContext, smDS, currentVersionEditionConfirmed, versionUpdateAssociated, versionDetails, "true")

		convey.So(err, convey.ShouldBeNil)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("The transition is not successful", t, func() {
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

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "state not allowed to transition")
	})
}
