package mongo

import (
	"context"
	kmongo "github.com/taxime-hq/kit/mongo"
	"log"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	url    = "127.0.0.1:27017"
	dbName = "mydb_test"
	cName  = "session"
)

func TestStore(t *testing.T) {
	mongoClient := kmongo.NewMongoClient([]string{url})
	err := mongoClient.Connect(time.Second * 6)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := mongoClient.Disconnect()
		if err != nil {
			log.Panicf("can not disconnect from mongo, due to %v", err)
			return
		}
	}()

	mstore := newManagerStore(mongoClient, dbName, cName)
	defer mstore.Close()

	Convey("Test mongo storage operation", t, func() {
		store, err := mstore.Create(context.Background(), "test_mongo_store", 10)
		So(err, ShouldBeNil)
		foo, ok := store.Get("foo")
		So(ok, ShouldBeFalse)
		So(foo, ShouldBeNil)

		store.Set("foo", "bar")
		store.Set("foo2", "bar2")
		err = store.Save()
		So(err, ShouldBeNil)

		foo, ok = store.Get("foo")
		So(ok, ShouldBeTrue)
		So(foo, ShouldEqual, "bar")

		foo = store.Delete("foo")
		So(foo, ShouldEqual, "bar")

		foo, ok = store.Get("foo")
		So(ok, ShouldBeFalse)
		So(foo, ShouldBeNil)

		foo2, ok := store.Get("foo2")
		So(ok, ShouldBeTrue)
		So(foo2, ShouldEqual, "bar2")

		err = store.Flush()
		So(err, ShouldBeNil)

		foo2, ok = store.Get("foo2")
		So(ok, ShouldBeFalse)
		So(foo2, ShouldBeNil)
	})
}

func TestManagerStore(t *testing.T) {
	mongoClient := kmongo.NewMongoClient([]string{url})
	err := mongoClient.Connect(time.Second * 6)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := mongoClient.Disconnect()
		if err != nil {
			log.Panicf("can not disconnect from mongo, due to %v", err)
			return
		}
	}()

	mstore := newManagerStore(mongoClient, dbName, cName)
	defer mstore.Close()

	Convey("Test mongo-based storage management operations", t, func() {
		sid := "test_manager_store"
		store, err := mstore.Create(context.Background(), sid, 10)
		So(store, ShouldNotBeNil)
		So(err, ShouldBeNil)

		store.Set("foo", "bar")
		err = store.Save()
		So(err, ShouldBeNil)

		store, err = mstore.Update(context.Background(), sid, 10)
		So(store, ShouldNotBeNil)
		So(err, ShouldBeNil)

		foo, ok := store.Get("foo")
		So(ok, ShouldBeTrue)
		So(foo, ShouldEqual, "bar")

		newsid := "test_manager_store2"
		store, err = mstore.Refresh(context.Background(), sid, newsid, 10)
		So(store, ShouldNotBeNil)
		So(err, ShouldBeNil)

		foo, ok = store.Get("foo")
		So(ok, ShouldBeTrue)
		So(foo, ShouldEqual, "bar")

		exists, err := mstore.Check(context.Background(), sid)
		So(exists, ShouldBeFalse)
		So(err, ShouldBeNil)

		err = mstore.Delete(context.Background(), newsid)
		So(err, ShouldBeNil)

		exists, err = mstore.Check(context.Background(), newsid)
		So(exists, ShouldBeFalse)
		So(err, ShouldBeNil)
	})
}
