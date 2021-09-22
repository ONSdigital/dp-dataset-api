package main_test

import (
	"context"
	"testing"

	nomis "github.com/ONSdigital/dp-dataset-api/NOMIS"
	. "github.com/smartystreets/goconvey/convey"
)

var ctx context.Context

func TestCheckSubString(t *testing.T) {
	cases := []struct {
		Description    string
		GivenString    string
		ExpectedResult string
	}{
		{
			"Given a string with http and [Statistical Disclosure Control]",
			"you can get the information from " +
				"http://www.ons.gov.uk/statistical-disclosure-control/index.html[Statistical Disclosure Control] page on the ONS web site.",
			"you can get the information from [Statistical Disclosure Control]" +
				"(http://www.ons.gov.uk/statistical-disclosure-control/index.html) page on the ONS web site.",
		},
		{
			"Given a string with multiple http and [Statistical Disclosure Control]",
			"you can get the information from " +
				"http://www.ons.gov.uk/statistical-disclosure-control/index.html[Statistical Disclosure Control] and " +
				"http://www.nomis/indx.html[Statistical Disclosure Control] page on the ONS web site.",
			"you can get the information from [Statistical Disclosure Control]" +
				"(http://www.ons.gov.uk/statistical-disclosure-control/index.html) and [Statistical Disclosure Control](http://www.nomis/indx.html) page on the ONS web site.",
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
		{
			"Given a string no matches",
			"you can get the information from " +
				"hello world",
			"you can get the information from " +
				"hello world",
		},
	}

	for _, test := range cases {
		Convey(test.Description, t, func() {
			Convey("Then the CheckSubString function should return the expected string", func() {
				actualString := nomis.CheckSubString(test.GivenString, ctx)
				So(actualString, ShouldResemble, test.ExpectedResult)
			})
		})
	}
}
func TestCheckTitle(t *testing.T) {
	cases := []struct {
		Description    string
		GivenString    string
		ExpectedResult string
	}{
		{
			"Given a title ",
			"QS102EW - Population density",
			"Population density",
		},
		{
			"Given a title with multiple hyphens",
			"OT102EW - Population density (Out of term-time population)",
			"Population density (Out of term-time population)",
		},
		{
			"Given a title with multiple hyphens",
			"DC6104EWla - Industry by sex by age - Communal establishment residents",
			"Industry by sex by age - Communal establishment residents",
		},
		{
			"Given a title with multiple hyphens and multiple space",
			"DC6104EWla to AW1234WE -  Industry by sex by age - Communal establishment residents",
			"Industry by sex by age - Communal establishment residents",
		},
		{
			"Given a title with no space before the first hyphens",
			"AW1234WE-  Industry by sex by age - Communal establishment residents",
			"Industry by sex by age - Communal establishment residents",
		},
		{
			"Given a string no matches",
			"you can get the information from " +
				"hello world",
			"you can get the information from " +
				"hello world",
		},
	}

	for _, test := range cases {
		Convey(test.Description, t, func() {
			Convey("Then the CheckTitle function should return the expected string", func() {
				actualString := nomis.CheckTitle(test.GivenString, ctx)
				So(actualString, ShouldResemble, test.ExpectedResult)
			})
		})
	}
}
