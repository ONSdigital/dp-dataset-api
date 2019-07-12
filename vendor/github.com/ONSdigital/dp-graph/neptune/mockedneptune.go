package neptune

import (
	"github.com/ONSdigital/dp-graph/neptune/internal"
)

/*
This module provides the MockDB factory function to make a NeptuneDB into
which a mocked implementation of the gremgo driver's Pool may be injected
to avoid real database access.
*/

import (
	"github.com/ONSdigital/dp-graph/neptune/driver"
)

// mockDB provides a NeptuneDB, into which you can pass a mocked
// NeptunePoolMock implementation, and thus write tests that bypass real
// database communication.
func mockDB(poolMock *internal.NeptunePoolMock) *NeptuneDB {
	driver := driver.NeptuneDriver{Pool: poolMock}
	db := &NeptuneDB{driver, 5, 30}
	return db
}
