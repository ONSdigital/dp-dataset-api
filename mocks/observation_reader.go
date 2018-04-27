// Code generated by moq; DO NOT EDIT
// github.com/matryer/moq

package mocks

import (
	"sync"
)

var (
	lockCSVRowReaderMockClose sync.RWMutex
	lockCSVRowReaderMockRead  sync.RWMutex
)

// CSVRowReaderMock is a mock implementation of CSVRowReader.
//
//     func TestSomethingThatUsesCSVRowReader(t *testing.T) {
//
//         // make and configure a mocked CSVRowReader
//         mockedCSVRowReader := &CSVRowReaderMock{
//             CloseFunc: func() error {
// 	               panic("TODO: mock out the Close method")
//             },
//             ReadFunc: func() (string, error) {
// 	               panic("TODO: mock out the Read method")
//             },
//         }
//
//         // TODO: use mockedCSVRowReader in code that requires CSVRowReader
//         //       and then make assertions.
//
//     }
type CSVRowReaderMock struct {
	// CloseFunc mocks the Close method.
	CloseFunc func() error

	// ReadFunc mocks the Read method.
	ReadFunc func() (string, error)

	// calls tracks calls to the methods.
	calls struct {
		// Close holds details about calls to the Close method.
		Close []struct {
		}
		// Read holds details about calls to the Read method.
		Read []struct {
		}
	}
}

// Close calls CloseFunc.
func (mock *CSVRowReaderMock) Close() error {
	if mock.CloseFunc == nil {
		panic("moq: CSVRowReaderMock.CloseFunc is nil but CSVRowReader.Close was just called")
	}
	callInfo := struct {
	}{}
	lockCSVRowReaderMockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	lockCSVRowReaderMockClose.Unlock()
	return mock.CloseFunc()
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//     len(mockedCSVRowReader.CloseCalls())
func (mock *CSVRowReaderMock) CloseCalls() []struct {
} {
	var calls []struct {
	}
	lockCSVRowReaderMockClose.RLock()
	calls = mock.calls.Close
	lockCSVRowReaderMockClose.RUnlock()
	return calls
}

// Read calls ReadFunc.
func (mock *CSVRowReaderMock) Read() (string, error) {
	if mock.ReadFunc == nil {
		panic("moq: CSVRowReaderMock.ReadFunc is nil but CSVRowReader.Read was just called")
	}
	callInfo := struct {
	}{}
	lockCSVRowReaderMockRead.Lock()
	mock.calls.Read = append(mock.calls.Read, callInfo)
	lockCSVRowReaderMockRead.Unlock()
	return mock.ReadFunc()
}

// ReadCalls gets all the calls that were made to Read.
// Check the length with:
//     len(mockedCSVRowReader.ReadCalls())
func (mock *CSVRowReaderMock) ReadCalls() []struct {
} {
	var calls []struct {
	}
	lockCSVRowReaderMockRead.RLock()
	calls = mock.calls.Read
	lockCSVRowReaderMockRead.RUnlock()
	return calls
}
