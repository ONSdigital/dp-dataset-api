package mongo

import (
	"context"

	"github.com/ONSdigital/log.go/log"
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
		log.Event(ctx, "error counting items", log.ERROR, log.Error(err))
		return
	}

	// query the items corresponding to the provided offest and limit (only if necessary)
	if totalCount > 0 && limit > 0 && offset < totalCount {
		iter := q.Skip(offset).Limit(limit).Iter()
		defer func() {
			err := iter.Close()
			if err != nil {
				log.Event(ctx, "error closing iterator", log.ERROR, log.Error(err), log.Data{"query": q})
			}
		}()

		err = iter.All(result)
		if err != nil {
			return 0, err
		}
	}

	return totalCount, nil
}
