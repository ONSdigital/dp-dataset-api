package application

import (
	"context"
	neturl "net/url"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/url"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	codeListAPIURL     = &neturl.URL{Scheme: "http", Host: "localhost:22400"}
	datasetAPIURL      = &neturl.URL{Scheme: "http", Host: "localhost:22000"}
	downloadServiceURL = &neturl.URL{Scheme: "http", Host: "localhost:23600"}
	importAPIURL       = &neturl.URL{Scheme: "http", Host: "localhost:21800"}
	publicWebsiteURL   = &neturl.URL{Scheme: "http", Host: "localhost:20000"}
	privateWebsiteURL  = &neturl.URL{Scheme: "http", Host: "localhost:20000"}
	apiRouterPublicURL = &neturl.URL{Scheme: "http", Host: "localhost:23200", Path: "v1"}
	urlBuilder         = url.NewBuilder(publicWebsiteURL, privateWebsiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)
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

		approvedState, approvedOk := castStateToState("approved")
		So(approvedState.Name, ShouldEqual, Approved.Name)
		So(approvedOk, ShouldBeTrue)

		publishFailedState, publishFailedOk := castStateToState("publish_failed")
		So(publishFailedState.Name, ShouldEqual, PublishFailed.Name)
		So(publishFailedOk, ShouldBeTrue)

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
	smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine, nil, nil, false, urlBuilder, nil)

	Convey("The transition is successful", t, func() {
		err := smDS.StateMachine.Transition(testContext, smDS, currentVersionEditionConfirmed, versionUpdateAssociated, versionDetails, "true", nil, "")

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

		err := smDS.StateMachine.Transition(testContext, smDS, currentIncorrectState, incorrectStateVersion, versionDetails, "true", nil, "")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "state not allowed to transition")
	})

	Convey("The transition handles idempotent state changes correctly", t, func() {
		currentVersionApproved := &models.Version{
			State:       "published",
			ReleaseDate: "2024-12-31",
			Version:     1,
			ID:          "789",
		}

		versionUpdateApproved := &models.Version{
			State:       "published",
			ReleaseDate: "2024-12-31",
			Version:     1,
			ID:          "789",
		}

		err := smDS.StateMachine.Transition(testContext, smDS, currentVersionApproved, versionUpdateApproved, versionDetails, "true", nil, "")

		So(err, ShouldBeNil)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
	})
}
