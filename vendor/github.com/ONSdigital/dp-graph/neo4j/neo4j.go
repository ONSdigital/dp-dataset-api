package neo4j

import (
	"math"
	"math/rand"
	"strings"
	"time"

	graph "github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/dp-graph/neo4j/driver"
	"github.com/ONSdigital/go-ns/log"
	neoErrors "github.com/ONSdigital/golang-neo4j-bolt-driver/errors"
	"github.com/ONSdigital/golang-neo4j-bolt-driver/structures/messages"
)

const transientErrorPrefix = "Neo.TransientError"

// Neo4j is a database driver allowing connection to Neo4j
// and providing implementations of the functions defined
// by the graph/driver.go interfaces
type Neo4j struct {
	driver.Neo4jDriver

	maxRetries int
	timeout    int
}

// New sets reasonable neo4j specific defaults and instantiates a driver
func New(dbAddr string, size, timeout, retries int) (n *Neo4j, err error) {
	//set defaults if not provided
	if size == 0 {
		size = 30
	}

	if timeout == 0 {
		timeout = 60
	}

	if retries == 0 {
		retries = 5
	}

	d, err := driver.New(dbAddr, size, timeout)
	if err != nil {
		return nil, err
	}

	return &Neo4j{
		d,
		retries,
		timeout,
	}, nil
}

func (n *Neo4j) checkAttempts(err error, instanceID string, attempt int) error {
	if !isTransientError(err) {
		log.Info("received an error from neo4j that cannot be retried",
			log.Data{"instance_id": instanceID, "error": err})

		return graph.ErrNonRetriable{err}
	}

	time.Sleep(getSleepTime(attempt, 20*time.Millisecond))

	if attempt >= n.maxRetries {
		return graph.ErrAttemptsExceededLimit{err}
	}

	return nil
}

func isTransientError(err error) bool {
	var neoErr string
	var boltErr *neoErrors.Error
	var ok bool

	if boltErr, ok = err.(*neoErrors.Error); ok {
		if failureMessage, ok := boltErr.Inner().(messages.FailureMessage); ok {
			if neoErr, ok = failureMessage.Metadata["code"].(string); !ok {
				return false
			}
		}
	}

	if strings.Contains(neoErr, transientErrorPrefix) {
		return true
	}

	return false
}

// getSleepTime will return a sleep time based on the attempt and initial retry time.
// It uses the algorithm 2^n where n is the attempt number (double the previous) and
// a randomization factor of between 0-5ms so that the server isn't being hit constantly
// at the same time by many clients
func getSleepTime(attempt int, retryTime time.Duration) time.Duration {
	n := (math.Pow(2, float64(attempt)))
	rand.Seed(time.Now().Unix())
	rnd := time.Duration(rand.Intn(4)+1) * time.Millisecond
	return (time.Duration(n) * retryTime) - rnd
}
