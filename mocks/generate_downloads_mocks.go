// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mocks

import (
	"sync"
)


// KafkaProducerMock is a mock implementation of download.KafkaProducer.
//
// 	func TestSomethingThatUsesKafkaProducer(t *testing.T) {
//
// 		// make and configure a mocked download.KafkaProducer
// 		mockedKafkaProducer := &KafkaProducerMock{
// 			OutputFunc: func() chan []byte {
// 				panic("mock out the Output method")
// 			},
// 		}
//
// 		// use mockedKafkaProducer in code that requires download.KafkaProducer
// 		// and then make assertions.
//
// 	}
type KafkaProducerMock struct {
	// OutputFunc mocks the Output method.
	OutputFunc func() chan []byte

	// calls tracks calls to the methods.
	calls struct {
		// Output holds details about calls to the Output method.
		Output []struct {
		}
	}
	lockOutput sync.RWMutex
}

// Output calls OutputFunc.
func (mock *KafkaProducerMock) Output() chan []byte {
	if mock.OutputFunc == nil {
		panic("KafkaProducerMock.OutputFunc: method is nil but KafkaProducer.Output was just called")
	}
	callInfo := struct {
	}{}
	mock.lockOutput.Lock()
	mock.calls.Output = append(mock.calls.Output, callInfo)
	mock.lockOutput.Unlock()
	return mock.OutputFunc()
}

// OutputCalls gets all the calls that were made to Output.
// Check the length with:
//     len(mockedKafkaProducer.OutputCalls())
func (mock *KafkaProducerMock) OutputCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockOutput.RLock()
	calls = mock.calls.Output
	mock.lockOutput.RUnlock()
	return calls
}


// GenerateDownloadsEventMock is a mock implementation of download.GenerateDownloadsEvent.
//
// 	func TestSomethingThatUsesGenerateDownloadsEvent(t *testing.T) {
//
// 		// make and configure a mocked download.GenerateDownloadsEvent
// 		mockedGenerateDownloadsEvent := &GenerateDownloadsEventMock{
// 			MarshalFunc: func(s interface{}) ([]byte, error) {
// 				panic("mock out the Marshal method")
// 			},
// 		}
//
// 		// use mockedGenerateDownloadsEvent in code that requires download.GenerateDownloadsEvent
// 		// and then make assertions.
//
// 	}
type GenerateDownloadsEventMock struct {
	// MarshalFunc mocks the Marshal method.
	MarshalFunc func(s interface{}) ([]byte, error)

	// calls tracks calls to the methods.
	calls struct {
		// Marshal holds details about calls to the Marshal method.
		Marshal []struct {
			// S is the s argument value.
			S interface{}
		}
	}
	lockMarshal sync.RWMutex
}

// Marshal calls MarshalFunc.
func (mock *GenerateDownloadsEventMock) Marshal(s interface{}) ([]byte, error) {
	if mock.MarshalFunc == nil {
		panic("GenerateDownloadsEventMock.MarshalFunc: method is nil but GenerateDownloadsEvent.Marshal was just called")
	}
	callInfo := struct {
		S interface{}
	}{
		S: s,
	}
	mock.lockMarshal.Lock()
	mock.calls.Marshal = append(mock.calls.Marshal, callInfo)
	mock.lockMarshal.Unlock()
	return mock.MarshalFunc(s)
}

// MarshalCalls gets all the calls that were made to Marshal.
// Check the length with:
//     len(mockedGenerateDownloadsEvent.MarshalCalls())
func (mock *GenerateDownloadsEventMock) MarshalCalls() []struct {
	S interface{}
} {
	var calls []struct {
		S interface{}
	}
	mock.lockMarshal.RLock()
	calls = mock.calls.Marshal
	mock.lockMarshal.RUnlock()
	return calls
}
