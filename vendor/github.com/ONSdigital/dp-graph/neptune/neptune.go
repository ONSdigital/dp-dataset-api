package neptune

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/ONSdigital/dp-graph/neptune/driver"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gedge/graphson"
)

type NeptuneDB struct {
	driver.NeptuneDriver

	maxAttempts int
	timeout     int
}

func New(dbAddr string, size, timeout, retries int, errs chan error) (n *NeptuneDB, err error) {
	// set defaults if not provided
	if size == 0 {
		size = 30
	}
	if timeout == 0 {
		timeout = 30
	}
	if retries == 0 {
		retries = 5
	}

	var d *driver.NeptuneDriver
	if d, err = driver.New(context.Background(), dbAddr, errs); err != nil {
		return
	}

	// seed for sleepy() below
	rand.Seed(time.Now().Unix())

	n = &NeptuneDB{
		*d,
		1 + retries,
		timeout,
	}
	return
}

func (n *NeptuneDB) getVertices(gremStmt string) (vertices []graphson.Vertex, err error) {
	logData := log.Data{"fn": "getVertices", "statement": gremStmt, "attempt": 1}

	var res interface{}
	for attempt := 1; attempt < n.maxAttempts; attempt++ {
		if attempt > 1 {
			log.ErrorC("will retry", err, logData)
			sleepy(attempt, 20*time.Millisecond)
			logData["attempt"] = attempt
		}
		res, err = n.Pool.Get(gremStmt, nil, nil)
		if err == nil {
			var ok bool
			if vertices, ok = res.([]graphson.Vertex); !ok {
				err = errors.New("cannot cast Get results to []Vertex")
				log.ErrorC("cast", err, logData)
				return
			}
			// success
			return
		}
		// XXX check err for non-retriable errors
		if !isTransientError(err) {
			return
		}
	}
	// ASSERT: failed all attempts
	log.ErrorC("maxAttempts reached", err, logData)
	err = ErrAttemptsExceededLimit{err}
	return
	return
}

func (n *NeptuneDB) getStringList(gremStmt string) (strings []string, err error) {
	logData := log.Data{"fn": "getStringList", "statement": gremStmt, "attempt": 1}

	for attempt := 1; attempt < n.maxAttempts; attempt++ {
		if attempt > 1 {
			log.ErrorC("will retry", err, logData)
			sleepy(attempt, 20*time.Millisecond)
			logData["attempt"] = attempt
		}
		strings, err = n.Pool.GetStringList(gremStmt, nil, nil)
		if err == nil {
			return
		}
		// XXX check err for non-retriable errors
		if !isTransientError(err) {
			return
		}
	}
	// ASSERT: failed all attempts
	log.ErrorC("maxAttempts reached", err, logData)
	err = ErrAttemptsExceededLimit{err}
	return
}


func (n *NeptuneDB) getVertex(gremStmt string) (vertex graphson.Vertex, err error) {
	logData := log.Data{"fn": "getVertex", "statement": gremStmt}

	var vertices []graphson.Vertex
	if vertices, err = n.getVertices(gremStmt); err != nil {
		log.ErrorC("get", err, logData)
		return
	}
	if len(vertices) != 1 {
		err = errors.New("expected one vertex")
		log.ErrorC("not one", err, logData)
		return
	}
	return vertices[0], nil
}

func (n *NeptuneDB) getEdges(gremStmt string) (edges []graphson.Edge, err error) {
	logData := log.Data{"fn": "getEdges", "statement": gremStmt, "attempt": 1}

	var res interface{}
	for attempt := 1; attempt < n.maxAttempts; attempt++ {
		if attempt > 1 {
			log.ErrorC("will retry", err, logData)
			sleepy(attempt, 20*time.Millisecond)
			logData["attempt"] = attempt
		}
		res, err = n.Pool.GetE(gremStmt, nil, nil)
		if err == nil {
			// success
			var ok bool
			if edges, ok = res.([]graphson.Edge); !ok {
				err = errors.New("cannot cast GetE results to []Edge")
				log.ErrorC("cast", err, logData)
				return
			}
			// return re-cast success
			return
		}
		// XXX check err for non-retriable errors
		if !isTransientError(err) {
			return
		}
	}
	// ASSERT: failed all attempts
	log.ErrorC("maxAttempts reached", err, logData)
	err = ErrAttemptsExceededLimit{err}
	return
}

func (n *NeptuneDB) exec(gremStmt string) (res interface{}, err error) {
	logData := log.Data{"fn": "n.exec", "statement": gremStmt, "attempt": 1}

	for attempt := 1; attempt < n.maxAttempts; attempt++ {
		if attempt > 1 {
			log.ErrorC("will retry", err, logData)
			sleepy(attempt, 20*time.Millisecond)
			logData["attempt"] = attempt
		}
		if res, err = n.Pool.Execute(gremStmt, nil, nil); err == nil {
			// success
			if res == nil {
				err = errors.New("res returned nil")
				log.ErrorC("bad res", err, logData)
				return
			}
			logData["exec_res"] = res
			log.Info("exec ok", logData)
			return
		}
		// XXX check err more thoroughly (isTransientError?) (non-err failures?)
		if !isTransientError(err) {
			return
		}
	}
	// ASSERT: failed all attempts
	log.ErrorC("maxAttempts reached", err, logData)
	err = ErrAttemptsExceededLimit{err}
	return
}

func (n *NeptuneDB) getNumber(gremStmt string) (count int64, err error) {
	logData := log.Data{"fn": "n.getNumber", "statement": gremStmt, "attempt": 1}

	for attempt := 1; attempt < n.maxAttempts; attempt++ {
		if attempt > 1 {
			log.ErrorC("will retry", err, logData)
			sleepy(attempt, 20*time.Millisecond)
			logData["attempt"] = attempt
		}
		if count, err = n.Pool.GetCount(gremStmt, nil, nil); err == nil {
			// success, so return number
			return
		}
		// XXX check non-nil err more thoroughly (isTransientError?)
		if !isTransientError(err) {
			return
		}
	}
	// ASSERT: failed all attempts
	log.ErrorC("maxAttempts reached", err, logData)
	err = ErrAttemptsExceededLimit{err}
	return
}

// ErrAttemptsExceededLimit is returned when the number of attempts has reached
// the maximum permitted
type ErrAttemptsExceededLimit struct {
	WrappedErr error
}

func (e ErrAttemptsExceededLimit) Error() string {
	return fmt.Sprintf("number of attempts to execute statement exceeded: %s", e.WrappedErr.Error())
}

/*
func (n *Neptune) checkAttempts(err error, instanceID string, attempt int) error {
	if !isTransientError(err) {
		log.Info("received an error from neptune that cannot be retried",
			log.Data{"instance_id": instanceID, "error": err})

		return err
	}

	time.Sleep(getSleepTime(attempt, 20*time.Millisecond))

	if attempt >= n.maxRetries {
		return ErrAttemptsExceededLimit{err}
	}

	return nil
}
*/
func isTransientError(err error) bool {
	if strings.Contains(err.Error(), " MALFORMED REQUEST ") ||
		strings.Contains(err.Error(), " INVALID REQUEST ARGUMENTS ") {
		return false
	}
	return true
}

// sleepy sleeps for a time which increases, based on the attempt and initial retry time.
// It uses the algorithm 2^n where n is the attempt number (double the previous) and
// a randomization factor of between 0-5ms so that the server isn't being hit constantly
// at the same time by many clients
func sleepy(attempt int, retryTime time.Duration) {
	n := (math.Pow(2, float64(attempt)))
	rnd := time.Duration(rand.Intn(4)+1) * time.Millisecond
	time.Sleep((time.Duration(n) * retryTime) - rnd)
}
