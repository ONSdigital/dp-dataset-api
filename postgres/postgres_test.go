package postgres

import (
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/smartystreets/goconvey/convey"
)

const getDatasetsSQL = "SELECT (.+) FROM Datasets JOIN Contacts (.+)"

func TestNewPostgresDatastore(t *testing.T) {
	Convey("When creating a postgres datastore no errors are returned", t, func() {
		mock, db := NewSQLMockWithSQLStatements()
		_, err := NewDatastore(db)
		So(err, ShouldBeNil)
		So(mock.ExpectationsWereMet(), ShouldBeNil)
	})
}

func NewSQLMockWithSQLStatements() (sqlmock.Sqlmock, *sql.DB) {
	db, mock, err := sqlmock.New()
	So(err, ShouldBeNil)
	mock.ExpectBegin()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectPrepare(getDatasetsSQL)
	_, dbError := db.Begin()
	So(dbError, ShouldBeNil)
	return mock, db
}

func TestDatastore_GetAllDatasets(t *testing.T) {
	Convey("", t, func() {
		mock, db := NewSQLMockWithSQLStatements()
		ds, err := NewDatastore(db)
		So(err, ShouldBeNil)
		mock.ExpectQuery(getDatasetsSQL).WillReturnRows(sqlmock.NewRows([]string{"datasetid", "Datasets.name", "nextrelease", "Contacts.name", "telephone", "email"}).
			AddRow("123", "CPI", "000000000", "name123", "01234 123456", "user@ons.gov.uk"))
		results, err := ds.GetAllDatasets()
		So(err, ShouldBeNil)
		So(len(results.Items), ShouldEqual, 1)
		So(mock.ExpectationsWereMet(), ShouldBeNil)
	})
}

func TestDatastore_GetAllDatasetsReturnsError(t *testing.T) {
	Convey("", t, func() {
		mock, db := NewSQLMockWithSQLStatements()
		ds, err := NewDatastore(db)
		So(err, ShouldBeNil)
		mock.ExpectQuery(getDatasetsSQL).WillReturnError(sql.ErrNoRows)
		results, err := ds.GetAllDatasets()
		So(results, ShouldEqual, nil)
		So(err, ShouldEqual, sql.ErrNoRows)
		So(mock.ExpectationsWereMet(), ShouldBeNil)
	})
}