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
	Convey("AddVersionDetailsToInstance completes successfully", t, func() {
		res := &mocks.BoltResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{
						"properties-set": int64(3),
					},
				}
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

		So(len(res.MetadataCalls()), ShouldEqual, 1)
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

	Convey("given result.Metadata() stats are not as expected", t, func() {
		res := &mocks.BoltResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": "invalid stats",
				}
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
			So(err.Error(), ShouldEqual, "neoClient AddVersionDetailsToInstance: error getting query result stats")
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
			So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
			So(len(stmt.CloseCalls()), ShouldEqual, 1)
			So(len(res.MetadataCalls()), ShouldEqual, 1)
		})
	})

	Convey("given result stats do not contain 'properties-set'", t, func() {
		res := &mocks.BoltResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{},
				}
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
			So(err.Error(), ShouldEqual, "neoClient AddVersionDetailsToInstance: error verifying query results")
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
			So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
			So(len(stmt.CloseCalls()), ShouldEqual, 1)
			So(len(res.MetadataCalls()), ShouldEqual, 1)
		})
	})

	Convey("given result stats properties-set is not the expected format", t, func() {
		res := &mocks.BoltResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{
						"properties-set": "3",
					},
				}
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
			So(err.Error(), ShouldEqual, "neoClient AddVersionDetailsToInstance: error verifying query results")
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
			So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
			So(len(stmt.CloseCalls()), ShouldEqual, 1)
			So(len(res.MetadataCalls()), ShouldEqual, 1)
		})
	})

	Convey("given result stats properties-set is not the expected value", t, func() {
		res := &mocks.BoltResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{
						"properties-set": int64(666),
					},
				}
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
			So(err.Error(), ShouldEqual, "neoClient AddVersionDetailsToInstance: unexpected rows affected expected 3 but was 666")
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
			So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
			So(len(stmt.CloseCalls()), ShouldEqual, 1)
			So(len(res.MetadataCalls()), ShouldEqual, 1)
		})
	})
}

func TestNeo4j_SetInstanceIsPublishedSuccess(t *testing.T) {
	Convey("SetInstanceIsPublished completes successfully", t, func() {
		res := &mocks.BoltResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{
						"properties-set": int64(1),
					},
				}
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

		err := store.SetInstanceIsPublished(context.Background(), testInstanceID)
		So(err, ShouldBeNil)
		So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)

		So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
		So(conn.PrepareNeoCalls()[0].Query, ShouldEqual, fmt.Sprintf(setInstanceIsPublished, testInstanceID))
		So(len(conn.CloseCalls()), ShouldEqual, 1)

		So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
		So(stmt.ExecNeoCalls()[0].Params, ShouldBeNil)
		So(len(stmt.CloseCalls()), ShouldEqual, 1)

		So(len(res.MetadataCalls()), ShouldEqual, 1)
	})
}

func TestNeo4j_SetInstanceIsPublishedError(t *testing.T) {
	Convey("given OpenPool returns an error", t, func() {
		mockPool := &mocks.DBPoolMock{
			OpenPoolFunc: func() (golangNeo4jBoltDriver.Conn, error) {
				return nil, errTest
			},
		}

		store := Neo4j{Pool: mockPool}

		err := store.SetInstanceIsPublished(context.Background(), testInstanceID)

		Convey("then the expected error is returned", func() {
			So(err, ShouldResemble, errors.WithMessage(errTest, "neoClient SetInstanceIsPublished: error opening neo4j connection"))
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

		err := store.SetInstanceIsPublished(context.Background(), testInstanceID)

		Convey("then the expected error is returned", func() {
			So(err, ShouldResemble, errors.WithMessage(errTest, "neoClient SetInstanceIsPublished: error preparing neo update statement"))
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

		err := store.SetInstanceIsPublished(context.Background(), testInstanceID)

		Convey("then the expected error is returned", func() {
			So(err, ShouldResemble, errors.WithMessage(errTest, "neoClient SetInstanceIsPublished: error executing neo4j update statement"))
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
			So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
		})
	})

	Convey("given result.Metadata() stats are not as expected", t, func() {
		res := &mocks.BoltResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": "invalid stats",
				}
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

		err := store.SetInstanceIsPublished(context.Background(), testInstanceID)

		Convey("then the expected error is returned", func() {
			So(err.Error(), ShouldEqual, "neoClient SetInstanceIsPublished: error getting query result stats")
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
			So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
			So(len(stmt.CloseCalls()), ShouldEqual, 1)
			So(len(res.MetadataCalls()), ShouldEqual, 1)
		})
	})

	Convey("given result stats do not contain 'properties-set'", t, func() {
		res := &mocks.BoltResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{},
				}
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

		err := store.SetInstanceIsPublished(context.Background(), testInstanceID)

		Convey("then the expected error is returned", func() {
			So(err.Error(), ShouldEqual, "neoClient SetInstanceIsPublished: error verifying query results")
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
			So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
			So(len(stmt.CloseCalls()), ShouldEqual, 1)
			So(len(res.MetadataCalls()), ShouldEqual, 1)
		})
	})

	Convey("given result stats properties-set is not the expected format", t, func() {
		res := &mocks.BoltResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{
						"properties-set": "1",
					},
				}
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

		err := store.SetInstanceIsPublished(context.Background(), testInstanceID)

		Convey("then the expected error is returned", func() {
			So(err.Error(), ShouldEqual, "neoClient SetInstanceIsPublished: error verifying query results")
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
			So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
			So(len(stmt.CloseCalls()), ShouldEqual, 1)
			So(len(res.MetadataCalls()), ShouldEqual, 1)
		})
	})

	Convey("given result stats properties-set is not the expected value", t, func() {
		res := &mocks.BoltResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{
						"properties-set": int64(666),
					},
				}
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

		err := store.SetInstanceIsPublished(context.Background(), testInstanceID)

		Convey("then the expected error is returned", func() {
			So(err.Error(), ShouldEqual, "neoClient SetInstanceIsPublished: unexpected rows affected expected 1 but was 666")
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(conn.PrepareNeoCalls()), ShouldEqual, 1)
			So(len(conn.CloseCalls()), ShouldEqual, 1)
			So(len(stmt.ExecNeoCalls()), ShouldEqual, 1)
			So(len(stmt.CloseCalls()), ShouldEqual, 1)
			So(len(res.MetadataCalls()), ShouldEqual, 1)
		})
	})
}
