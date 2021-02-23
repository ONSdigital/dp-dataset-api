package main_test

import (
	nomis "github.com/ONSdigital/dp-dataset-api/NOMIS"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestCheckSubString(t *testing.T) {
	cases := []struct {
		Description    string
		GivenString    string
		ExpectedResult string
	}{
		{
			"Given a string with http and[Statistical Disclosure Control]",
			"you can get the information from " +
				"http://www.ons.gov.uk/statistical-disclosure-control/index.html[Statistical Disclosure Control] page on the ONS web site.",
			"you can get the information from [Statistical Disclosure Control]" +
				"(http://www.ons.gov.uk/statistical-disclosure-control/index.html) page on the ONS web site.",
		},
		{
			"Given a string without [Statistical Disclosure Control]",
			"you can get the information from " +
				"http://www.ons.gov.uk/statistical-disclosure-control/index.aspx page on the ONS web site.",
			"you can get the information from " +
				"http://www.ons.gov.uk/statistical-disclosure-control/index.aspx page on the ONS web site.",
		},
		{
			"Given a string without http and [Statistical Disclosure Control]",
			"you can get the information from " +
				"http://www.ons.gov.uk/statistical-disclosure-control/index.aspx page on the ONS web site.",
			"you can get the information from " +
				"http://www.ons.gov.uk/statistical-disclosure-control/index.aspx page on the ONS web site.",
		},
	}
	Convey("Then the CheckSubString function should return the expected string", t, func() {
		for _, test := range cases {
			Convey(test.Description, func() {
				actualString := nomis.CheckSubString(test.GivenString)
				So(actualString, ShouldResemble, test.ExpectedResult)
			})
		}
	})
}
