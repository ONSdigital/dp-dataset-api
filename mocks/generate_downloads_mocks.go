// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mocks

import (
	"sync"
)

var (
	lockKafkaProducerMockOutput sync.RWMutex
)

// KafkaProducerMock is a mock implementation of KafkaProducer.
//
//     func TestSomethingThatUsesKafkaProducer(t *testing.T) {
//
//         // make and configure a mocked KafkaProducer
//         mockedKafkaProducer := &KafkaProducerMock{
//             OutputFunc: func() chan []byte {
// 	               panic("TODO: mock out the Output method")
//             },
//         }
//
//         // TODO: use mockedKafkaProducer in code that requires KafkaProducer
//         //       and then make assertions.
//
//     }
type KafkaProducerMock struct {
	// OutputFunc mocks the Output method.
	OutputFunc func() chan []byte

	// calls tracks calls to the methods.
	calls struct {
		// Output holds details about calls to the Output method.
		Output []struct {
		}
	}
}

// Output calls OutputFunc.
func (mock *KafkaProducerMock) Output() chan []byte {
	if mock.OutputFunc == nil {
		panic("KafkaProducerMock.OutputFunc: method is nil but KafkaProducer.Output was just called")
	}
	callInfo := struct {
	}{}
	lockKafkaProducerMockOutput.Lock()
	mock.calls.Output = append(mock.calls.Output, callInfo)
	lockKafkaProducerMockOutput.Unlock()
	return mock.OutputFunc()
}

// OutputCalls gets all the calls that were made to Output.
// Check the length with:
//     len(mockedKafkaProducer.OutputCalls())
func (mock *KafkaProducerMock) OutputCalls() []struct {
} {
	var calls []struct {
	}
	lockKafkaProducerMockOutput.RLock()
	calls = mock.calls.Output
	lockKafkaProducerMockOutput.RUnlock()
	return calls
}

var (
	lockGenerateDownloadsEventMockMarshal sync.RWMutex
)

// GenerateDownloadsEventMock is a mock implementation of GenerateDownloadsEvent.
//
//     func TestSomethingThatUsesGenerateDownloadsEvent(t *testing.T) {
//
//         // make and configure a mocked GenerateDownloadsEvent
//         mockedGenerateDownloadsEvent := &GenerateDownloadsEventMock{
//             MarshalFunc: func(s interface{}) ([]byte, error) {
// 	               panic("TODO: mock out the Marshal method")
//             },
//         }
//
//         // TODO: use mockedGenerateDownloadsEvent in code that requires GenerateDownloadsEvent
//         //       and then make assertions.
//
//     }
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
	lockGenerateDownloadsEventMockMarshal.Lock()
	mock.calls.Marshal = append(mock.calls.Marshal, callInfo)
	lockGenerateDownloadsEventMockMarshal.Unlock()
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
	lockGenerateDownloadsEventMockMarshal.RLock()
	calls = mock.calls.Marshal
	lockGenerateDownloadsEventMockMarshal.RUnlock()
	return calls
}
