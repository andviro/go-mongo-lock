package lock_test

import (
	"os"
	"testing"
	"time"

	"github.com/andviro/go-mongo-lock"
	"gopkg.in/mgo.v2"
)

var sess *mgo.Session

func testMain(m *testing.M) int {
	uri := os.Getenv("TEST_MONGODB_URI")
	if uri == "" {
		uri = "mongodb://localhost"
	}
	var err error
	sess, err = mgo.Dial(uri)
	if err != nil {
		panic(err)
	}
	defer sess.Close()
	return m.Run()
}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func TestNewAndCheck(t *testing.T) {
	sess := sess.Clone()
	defer sess.Close()

	coll := sess.DB("").C("test_coll")
	l, err := lock.New(coll, "testID", 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer l.Release()
		time.Sleep(2000 * time.Millisecond)
	}()

	// Test Check
	err = lock.Check(coll, "testID")
	if err != lock.ErrLockBusy {
		t.Error("lock check failed")
	}

	// Test wait timeout
	_, err = lock.New(coll, "testID", 1000, 200)
	if err != lock.ErrWaitTimeout {
		t.Error("unexpected error:", err)
	}
	time.Sleep(1000 * time.Millisecond)

	// Test expiration
	_, err = lock.New(coll, "testID", 2000, 100)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	time.Sleep(1500 * time.Millisecond)
	// Test that new lock (2000ms) is not released by deferred lock.Release()
	// (lock key protection)
	if err = lock.Check(coll, "testID"); err != lock.ErrLockBusy {
		t.Error("lock should be busy:", err)
	}
}
