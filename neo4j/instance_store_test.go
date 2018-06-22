package neo4j

import (
	"context"
	"fmt"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/neo4j/mocks"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testInstanceID = "666"
	testDatasetId  = "123"
	testEdition    = "2018"
	testVersion    = 1
)

var errTest = errors.New("test")
var closeNoErr = func() error {
	return nil
}

func TestNeo4j_AddVersionDetailsToInstanceSuccess(t *testing.T) {
	Convey("AddVersionDetailsToInstanceSuccess completes successfully", t, func() {
		res := &mocks.BoltResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 1, nil
			},
		}
		stmt := &mocks.BoltStmtMock{
			CloseFunc: closeNoErr,
			ExecNeoFunc: func(params map[string]interface{}) (golangNeo4jBoltDriver.Result, error) {
				return res, nil
			},
		}
		conn := &mocks.BoltConnMock{
			CloseFunc: closeNoErr,
			PrepareNeoFunc: func(query string) (golangNeo4jBoltDriver.Stmt, error) {
				return stmt, nil
			},
		}
		mockPool := &mocks.DBPoolMock{
			OpenPoolFunc: func() (golangNeo4jBoltDriver.Conn, error) {
				return conn, nil
			},
		}

		store := Neo4j{Pool: mockPool}

		err := store.AddVersionDetailsToInstance(context.Background(), testInstanceID, testDatasetId, testEdition, testVersion)
		So(err, ShouldBeNil)
		So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)

		So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
		So(conn.PrepareNeoCalls()[0].Query, ShouldEqual, fmt.Sprintf(addVersionDetailsToInstance, testInstanceID))
		So(len(conn.CloseCalls()), ShouldEqual, 1)

		So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
		So(stmt.ExecNeoCalls()[0].Params, ShouldResemble, map[string]interface{}{
			"dataset_id": testDatasetId,
			"edition":    testEdition,
			"version":    testVersion,
		})
		So(len(stmt.CloseCalls()), ShouldEqual, 1)

		So(len(res.RowsAffectedCalls()), ShouldEqual, 1)
	})
}

func TestNeo4j_AddVersionDetailsToInstanceError(t *testing.T) {
	Convey("given OpenPool returns an error", t, func() {
		mockPool := &mocks.DBPoolMock{
			OpenPoolFunc: func() (golangNeo4jBoltDriver.Conn, error) {
				return nil, errTest
			},
		}

		store := Neo4j{Pool: mockPool}

		err := store.AddVersionDetailsToInstance(context.Background(), testInstanceID, testDatasetId, testEdition, testVersion)

		Convey("then the expected error is returned", func() {
			So(err, ShouldResemble, errors.WithMessage(errTest, "neoClient AddVersionDetailsToInstance: error opening neo4j connection"))
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
		})
	})

	Convey("given conn.PrepareNeo returns an error", t, func() {
		conn := &mocks.BoltConnMock{
			PrepareNeoFunc: func(query string) (golangNeo4jBoltDriver.Stmt, error) {
				return nil, errTest
			},
			CloseFunc: closeNoErr,
		}
		mockPool := &mocks.DBPoolMock{
			OpenPoolFunc: func() (golangNeo4jBoltDriver.Conn, error) {
				return conn, nil
			},
		}

		store := Neo4j{Pool: mockPool}

		err := store.AddVersionDetailsToInstance(context.Background(), testInstanceID, testDatasetId, testEdition, testVersion)

		Convey("then the expected error is returned", func() {
			So(err, ShouldResemble, errors.WithMessage(errTest, "neoClient AddVersionDetailsToInstance: error preparing neo update statement"))
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
		})
	})

	Convey("given stmt.ExecNeo returns an error", t, func() {
		stmt := &mocks.BoltStmtMock{
			ExecNeoFunc: func(params map[string]interface{}) (golangNeo4jBoltDriver.Result, error) {
				return nil, errTest
			},
			CloseFunc: closeNoErr,
		}
		conn := &mocks.BoltConnMock{
			PrepareNeoFunc: func(query string) (golangNeo4jBoltDriver.Stmt, error) {
				return stmt, nil
			},
			CloseFunc: closeNoErr,
		}
		mockPool := &mocks.DBPoolMock{
			OpenPoolFunc: func() (golangNeo4jBoltDriver.Conn, error) {
				return conn, nil
			},
		}

		store := Neo4j{Pool: mockPool}

		err := store.AddVersionDetailsToInstance(context.Background(), testInstanceID, testDatasetId, testEdition, testVersion)

		Convey("then the expected error is returned", func() {
			So(err, ShouldResemble, errors.WithMessage(errTest, "neoClient AddVersionDetailsToInstance: error executing neo4j update statement"))
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
			So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
		})
	})

	Convey("given result.RowsAffected() returns an error", t, func() {
		res := &mocks.BoltResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 0, errTest
			},
		}
		stmt := &mocks.BoltStmtMock{
			ExecNeoFunc: func(params map[string]interface{}) (golangNeo4jBoltDriver.Result, error) {
				return res, nil
			},
			CloseFunc: closeNoErr,
		}
		conn := &mocks.BoltConnMock{
			PrepareNeoFunc: func(query string) (golangNeo4jBoltDriver.Stmt, error) {
				return stmt, nil
			},
			CloseFunc: closeNoErr,
		}
		mockPool := &mocks.DBPoolMock{
			OpenPoolFunc: func() (golangNeo4jBoltDriver.Conn, error) {
				return conn, nil
			},
		}

		store := Neo4j{Pool: mockPool}

		err := store.AddVersionDetailsToInstance(context.Background(), testInstanceID, testDatasetId, testEdition, testVersion)

		Convey("then the expected error is returned", func() {
			So(err, ShouldResemble, errors.WithMessage(errTest, "neoClient AddVersionDetailsToInstance: error getting update result data"))
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
			So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
			So(len(stmt.CloseCalls()), ShouldEqual, 1)
			So(len(res.RowsAffectedCalls()), ShouldEqual, 1)
		})
	})

	Convey("given result.RowsAffected() returns more than 1", t, func() {
		res := &mocks.BoltResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 666, nil
			},
		}
		stmt := &mocks.BoltStmtMock{
			ExecNeoFunc: func(params map[string]interface{}) (golangNeo4jBoltDriver.Result, error) {
				return res, nil
			},
			CloseFunc: closeNoErr,
		}
		conn := &mocks.BoltConnMock{
			PrepareNeoFunc: func(query string) (golangNeo4jBoltDriver.Stmt, error) {
				return stmt, nil
			},
			CloseFunc: closeNoErr,
		}
		mockPool := &mocks.DBPoolMock{
			OpenPoolFunc: func() (golangNeo4jBoltDriver.Conn, error) {
				return conn, nil
			},
		}

		store := Neo4j{Pool: mockPool}

		err := store.AddVersionDetailsToInstance(context.Background(), testInstanceID, testDatasetId, testEdition, testVersion)

		Convey("then the expected error is returned", func() {
			So(err.Error(), ShouldResemble, "neoClient AddVersionDetailsToInstance: unexpected rows affected expected 1 but was 666")
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
			So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
			So(len(stmt.CloseCalls()), ShouldEqual, 1)
			So(len(res.RowsAffectedCalls()), ShouldEqual, 1)
		})
	})
}
