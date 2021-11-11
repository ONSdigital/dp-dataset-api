package mongo

import (
	"context"

	"github.com/ONSdigital/log.go/v2/log"

	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"

	"github.com/globalsign/mgo"
)

// QueryPage obtains the page of documents defined by the provided offset and limit into the provided slice
// and closes the iterator.
// It also provides the total count of documents that would satisfy the provided query without offset or limit.
//
// The result argument must necessarily be the address for a slice. The slice
// may be nil or previously allocated.
//
// For instance:
//
//    var result []struct{ Value int }
//    q := s.DB(myDatabase).C(myCollection).Find(mySelector)
//    totalCount, err := queryPage(ctx, q, offset, limit, &result)
//    if err != nil {
//        return err
//    }
//
func QueryPage(ctx context.Context, q *mgo.Query, offset, limit int, result interface{}) (totalCount int, err error) {

	// get total count of items for the provided query
	totalCount, err = q.Count()
	if err != nil {
		log.Error(ctx, "error counting items", err)
		return
	}

	// query the items corresponding to the provided offest and limit (only if necessary)
	if totalCount > 0 && limit > 0 && offset < totalCount {
		iter := q.Skip(offset).Limit(limit).Iter()
		defer func() {
			err := iter.Close()
			if err != nil {
				log.Error(ctx, "error closing iterator", err, log.Data{"query": q})
			}
		}()

		err = iter.All(result)
		if err != nil {
			return 0, err
		}
	}

	return totalCount, nil
}

// NewQueryPage obtains the page of documents defined by the provided offset and limit into the provided slice
// It also provides the total count of documents that would satisfy the provided query without offset or limit.
//
// The result argument must necessarily be the address for a slice. The slice
// may be nil or previously allocated.
//
// For instance:
//
//    var result []struct{ Value int }
//    f := m.Connection.GetConfiguredCollection().Find(mySelector)
//    totalCount, err := QueryPage(ctx, f, offset, limit, &result)
//    if err != nil {
//        return err
//    }
//
func NewQueryPage(ctx context.Context, f *mongodriver.Find, offset, limit int, result interface{}) (totalCount int, err error) {

	// get total count of items for the provided query
	totalCount, err = f.Count(ctx)
	if err != nil {
		log.Error(ctx, "error counting items", err)
		return 0, err
	}

	// query the items corresponding to the provided offset and limit (only if necessary)
	// guaranteeing at least one document will be found
	if totalCount > 0 && limit > 0 && offset < totalCount {
		return totalCount, f.Skip(offset).Limit(limit).IterAll(ctx, result)
	}

	return totalCount, nil
}
