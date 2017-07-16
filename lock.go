package lock

import (
	"errors"
	"math/rand"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Lock implements simple distributed lock that relies on MongoDB unique
// indexes. It needs a separate MongoDB collection to store the locks.

type Lock interface {
	// Release releases the lock provided that it's key is currently held in
	// database. Returns mgo.ErrNotFound or other error if lock is not being held.
	Release() error
}

type lock struct {
	ID       interface{} `bson:"_id"`
	Key      bson.ObjectId
	Deadline time.Time
	coll     *mgo.Collection
}

// ErrLockBusy means that the lock can not be acquired at the moment
var ErrLockBusy = errors.New("lock busy")

// ErrWaitTimeout means that the lock could not be acquired at the moment
var ErrWaitTimeout = errors.New("wait timeout")

func (l *lock) Release() error {
	return l.coll.Remove(bson.M{"_id": l.ID, "key": l.Key})
}

// Check if the lock on specified ID is currently held and not expired. Returns
// nil if lock is not set, ErrLockBusy or other mgo error otherwise.
func Check(coll *mgo.Collection, ID interface{}) (err error) {
	var lock lock
	now := bson.Now()
	switch err = coll.Find(bson.M{"_id": ID, "deadline": bson.M{"$gt": now}}).One(&lock); err {
	case mgo.ErrNotFound:
		return nil
	case nil:
		return ErrLockBusy
	}
	return
}

// Break forces immediate lock release
func Break(coll *mgo.Collection, ID interface{}) (err error) {
	err = coll.Remove(bson.M{"_id": ID})
	if err == mgo.ErrNotFound {
		err = nil
	}
	return
}

// New tries to lock specified ID with particular maximum age and wait timeout
// in milliseconds. Returns ErrWaitTimeout if failed to acquire while other
// process holds this lock ID.
func New(coll *mgo.Collection, ID interface{}, maxAge int64, waitTimeout int64) (res Lock, err error) {
	now := bson.Now()
	if err = coll.Remove(bson.M{"_id": ID, "deadline": bson.M{"$lt": now}}); err != nil {
		if err != mgo.ErrNotFound {
			return
		}
	}
	waitGranularity := waitTimeout / 10
	if waitGranularity < 50 {
		waitGranularity = 50
	} else if waitGranularity > 200 {
		waitGranularity = 200
	}
	maxWait := time.Now().Add(time.Duration(waitTimeout) * time.Millisecond)
	lock := &lock{ID: ID, Key: bson.NewObjectId(), coll: coll}
	res = lock
	for {
		err = nil
		now := bson.Now()
		lock.Deadline = now.Add(time.Duration(maxAge) * time.Millisecond)
		switch err = coll.Insert(lock); {
		case mgo.IsDup(err):
			break
		default:
			return
		}
		if now.After(maxWait) {
			err = ErrWaitTimeout
			return
		}
		jitter := time.Duration(rand.Int63n(waitGranularity/2)) * time.Millisecond
		time.Sleep(time.Duration(waitGranularity/2)*time.Millisecond + jitter)
	}
}
