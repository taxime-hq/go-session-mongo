package mongo

import (
	"context"
	"sync"
	"time"

	session "github.com/go-session/session/v3"
	jsoniter "github.com/json-iterator/go"
	"github.com/taxime-hq/kit/logging"
	kmongo "github.com/taxime-hq/kit/mongo"
	"github.com/taxime-hq/kit/tracing"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	_             session.ManagerStore = &managerStore{}
	_             session.Store        = &store{}
	jsonMarshal                        = jsoniter.Marshal
	jsonUnmarshal                      = jsoniter.Unmarshal
)

// NewStoreWithClient Create an instance of a mongo store
func NewStoreWithClient(mongoClient *kmongo.ClientV2, dbName, cName string) session.ManagerStore {
	return newManagerStore(mongoClient, dbName, cName)
}

func newManagerStore(mongoClient *kmongo.ClientV2, dbName, cName string) session.ManagerStore {
	collection := mongoClient.Default.Database(dbName).Collection(cName)
	indexOptions := options.Index().SetExpireAfterSeconds(1)
	indexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "expired_at", Value: 1}},
		Options: indexOptions,
	}

	_, err := collection.Indexes().CreateOne(context.Background(), indexModel)
	if err != nil {
		panic(err)
	}

	return &managerStore{
		mongoClient: mongoClient,
		dbName:      dbName,
		cName:       cName,
	}
}

type managerStore struct {
	mongoClient *kmongo.ClientV2
	dbName      string
	cName       string
}

func (s *managerStore) getValue(sid string) (string, error) {
	var item sessionItem
	err := s.getCollection().FindOne(context.Background(), bson.M{"_id": sid}).Decode(&item)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return "", nil
		}
		return "", err
	} else if item.ExpiredAt.Before(time.Now()) {
		return "", nil
	}
	return item.Value, nil
}

func (s *managerStore) parseValue(value string) (map[string]interface{}, error) {
	var values map[string]interface{}
	if len(value) > 0 {
		err := jsonUnmarshal([]byte(value), &values)
		if err != nil {
			return nil, err
		}
	}

	return values, nil
}

func (s *managerStore) Check(ctx context.Context, sid string) (bool, error) {
	defer tracing.MaybeStartSpan(&ctx).End()
	logging.DebugContext(ctx, "check session request",
		"sid", sid,
	)
	val, err := s.getValue(sid)
	if err != nil {
		logging.ErrorContext(ctx, "unable to check session",
			"sid", sid,
			"error", err,
		)
		return false, err
	}
	logging.DebugContext(ctx, "check session response",
		"exists", val != "",
	)
	return val != "", nil
}

// Create(ctx context.Context, sid string, expired int64) (Store, error)
func (s *managerStore) Create(ctx context.Context, sid string, expired int64) (session.Store, error) {
	defer tracing.MaybeStartSpan(&ctx).End()
	logging.DebugContext(ctx, "create session request",
		"sid", sid,
		"expired", expired,
	)
	st := newStore(ctx, s, sid, expired, nil)
	logging.DebugContext(ctx, "create session response",
		"sid", sid,
	)
	return st, nil
}

func (s *managerStore) Update(ctx context.Context, sid string, expired int64) (session.Store, error) {
	defer tracing.MaybeStartSpan(&ctx).End()
	logging.DebugContext(ctx, "update session request",
		"sid", sid,
		"expired", expired,
	)
	value, err := s.getValue(sid)
	if err != nil {
		logging.ErrorContext(ctx, "unable to update session",
			"sid", sid,
			"error", err,
		)
		return nil, err
	} else if value == "" {
		logging.DebugContext(ctx, "update session response",
			"sid", sid,
			"created", true,
		)
		return newStore(ctx, s, sid, expired, nil), nil
	}
	filter := bson.M{"_id": sid}
	update := bson.M{
		"$set": bson.M{
			"expired_at": time.Now().Add(time.Duration(expired) * time.Second),
		},
	}
	_, err = s.getCollection().UpdateOne(tracing.NewBackgroundContext(ctx), filter, update)
	if err != nil {
		logging.ErrorContext(ctx, "unable to update session",
			"sid", sid,
			"error", err,
		)
		return nil, err
	}

	values, err := s.parseValue(value)
	if err != nil {
		logging.ErrorContext(ctx, "unable to update session",
			"sid", sid,
			"error", err,
		)
		return nil, err
	}

	logging.DebugContext(ctx, "update session response",
		"sid", sid,
	)
	return newStore(ctx, s, sid, expired, values), nil
}

func (s *managerStore) Delete(ctx context.Context, sid string) error {
	defer tracing.MaybeStartSpan(&ctx).End()
	logging.DebugContext(ctx, "delete session request",
		"sid", sid,
	)
	_, err := s.getCollection().DeleteOne(tracing.NewBackgroundContext(ctx), bson.M{"_id": sid})
	if err != nil {
		logging.ErrorContext(ctx, "unable to delete session",
			"sid", sid,
			"error", err,
		)
		return err
	}
	logging.DebugContext(ctx, "delete session response",
		"success", true,
	)
	return nil
}

func (s *managerStore) Refresh(ctx context.Context, oldsid, sid string, expired int64) (session.Store, error) {
	defer tracing.MaybeStartSpan(&ctx).End()
	logging.DebugContext(ctx, "refresh session request",
		"old_sid", oldsid,
		"sid", sid,
		"expired", expired,
	)
	value, err := s.getValue(oldsid)
	if err != nil {
		logging.ErrorContext(ctx, "unable to refresh session",
			"old_sid", oldsid,
			"sid", sid,
			"error", err,
		)
		return nil, err
	} else if value == "" {
		logging.DebugContext(ctx, "refresh session response",
			"sid", sid,
			"created", true,
		)
		return newStore(ctx, s, sid, expired, nil), nil
	}
	filterNew := bson.M{"_id": sid}
	update := bson.M{
		"$set": bson.M{
			"_id":        sid,
			"value":      value,
			"expired_at": time.Now().Add(time.Duration(expired) * time.Second),
		},
	}
	_, err = s.getCollection().UpdateOne(tracing.NewBackgroundContext(ctx), filterNew, update, options.UpdateOne().SetUpsert(true))
	if err != nil {
		logging.ErrorContext(ctx, "unable to refresh session",
			"old_sid", oldsid,
			"sid", sid,
			"error", err,
		)
		return nil, err
	}

	filterOld := bson.M{"_id": oldsid}
	_, err = s.getCollection().DeleteOne(tracing.NewBackgroundContext(ctx), filterOld)
	if err != nil {
		logging.ErrorContext(ctx, "unable to refresh session",
			"old_sid", oldsid,
			"sid", sid,
			"error", err,
		)
		return nil, err
	}

	values, err := s.parseValue(value)
	if err != nil {
		logging.ErrorContext(ctx, "unable to refresh session",
			"old_sid", oldsid,
			"sid", sid,
			"error", err,
		)
		return nil, err
	}

	logging.DebugContext(ctx, "refresh session response",
		"sid", sid,
	)
	return newStore(ctx, s, sid, expired, values), nil
}

func (s *managerStore) getCollection() *mongo.Collection {
	return s.mongoClient.Default.Database(s.dbName).Collection(s.cName)
}

func (s *managerStore) Close() error {
	return nil
}

func newStore(ctx context.Context, s *managerStore, sid string, expired int64, values map[string]interface{}) session.Store {
	defer tracing.MaybeStartSpan(&ctx).End()
	logging.DebugContext(ctx, "new store input",
		"sid", sid,
		"expired", expired,
	)
	if values == nil {
		values = make(map[string]interface{})
	}

	logging.DebugContext(ctx, "new store output",
		"sid", sid,
	)
	return &store{
		mongoClient: s.mongoClient,
		dbName:      s.dbName,
		cName:       s.cName,
		ctx:         ctx,
		sid:         sid,
		expired:     expired,
		values:      values,
	}
}

type store struct {
	sync.RWMutex
	ctx         context.Context
	mongoClient *kmongo.ClientV2
	dbName      string
	cName       string
	sid         string
	expired     int64
	values      map[string]interface{}
}

func (s *store) Context() context.Context {
	return s.ctx
}

func (s *store) SessionID() string {
	return s.sid
}

func (s *store) Set(key string, value interface{}) {
	s.Lock()
	s.values[key] = value
	s.Unlock()
}

func (s *store) Get(key string) (interface{}, bool) {
	s.RLock()
	val, ok := s.values[key]
	s.RUnlock()
	return val, ok
}

func (s *store) Delete(key string) interface{} {
	s.RLock()
	v, ok := s.values[key]
	s.RUnlock()
	if ok {
		s.Lock()
		delete(s.values, key)
		s.Unlock()
	}
	return v
}

func (s *store) Flush() error {
	s.Lock()
	s.values = make(map[string]interface{})
	s.Unlock()
	return s.Save()
}

func (s *store) Save() error {
	var value string

	s.RLock()
	if len(s.values) > 0 {
		buf, err := jsonMarshal(s.values)
		if err != nil {
			s.RUnlock()
			return err
		}
		value = string(buf)
	}
	s.RUnlock()
	filter := bson.M{"_id": s.sid}
	update := bson.M{
		"$set": bson.M{
			"_id":        s.sid,
			"value":      value,
			"expired_at": time.Now().Add(time.Duration(s.expired) * time.Second),
		},
	}
	_, err := s.mongoClient.Default.Database(s.dbName).Collection(s.cName).UpdateOne(context.Background(), filter, update, options.UpdateOne().SetUpsert(true))

	return err
}

// Data items stored in mongo
type sessionItem struct {
	ID        string    `bson:"_id"`
	Value     string    `bson:"value"`
	ExpiredAt time.Time `bson:"expired_at"`
}
