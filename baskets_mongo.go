package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"net/http"
	"net/url"
	"regexp"
)

const DbTypeMongo = "mongo"

type (
	mongoBasket struct {
		Name         string            `bson:"name"`
		RequestCount int64             `bson:"requestCount"`
		Token        string            `bson:"token"`
		ConfigStruct mongoBasketConfig `bson:"config"`
		db           *mongoDatabase
	}

	mongoDatabase struct {
		db *mongo.Database
	}

	mongoBasketConfig struct {
		ForwardURL    string `bson:"forwardURL"`
		ProxyResponse bool   `bson:"proxyResponse"`
		InsecureTLS   bool   `bson:"insecureTLS"`
		ExpandPath    bool   `bson:"expandPath"`
		Capacity      int    `bson:"capacity"`

		ResponseConfigs map[string]mongoResponseConfig `bson:"responseConfigs"`
	}

	mongoResponseConfig struct {
		Status     int         `bson:"status"`
		Headers    http.Header `bson:"headers"`
		Body       string      `bson:"body"`
		IsTemplate bool        `bson:"isTemplate"`
	}

	mongoRequestData struct {
		Basket        string             `bson:"basket"`
		ID            primitive.ObjectID `bson:"_id"`
		Header        http.Header        `bson:"headers"`
		ContentLength int64              `bson:"contentLength"`
		Body          string             `bson:"body"`
		Method        string             `bson:"method"`
		Path          string             `bson:"path"`
		Query         string             `bson:"query"`
	}
)

var _ Basket = (*mongoBasket)(nil)
var _ BasketsDatabase = (*mongoDatabase)(nil)

func (m *mongoBasket) Config() BasketConfig {
	return BasketConfig{
		ForwardURL:    m.ConfigStruct.ForwardURL,
		ProxyResponse: m.ConfigStruct.ProxyResponse,
		InsecureTLS:   m.ConfigStruct.InsecureTLS,
		ExpandPath:    m.ConfigStruct.ExpandPath,
		Capacity:      m.ConfigStruct.Capacity,
	}
}

func (m *mongoBasket) Update(config BasketConfig) {
	_, err := m.db.getBasketCollection().UpdateOne(
		context.Background(),
		bson.M{"name": m.Name},
		bson.M{"$set": bson.M{
			"config.forwardURL":    config.ForwardURL,
			"config.proxyResponse": config.ProxyResponse,
			"config.insecureTLS":   config.InsecureTLS,
			"config.expandPath":    config.ExpandPath,
			"config.capacity":      config.Capacity,
		}},
	)
	if err != nil {
		log.Printf("[error] failed to update basket '%s' in database: %s", m.Name, err)
	}
}

func (m *mongoBasket) Authorize(token string) bool {
	return m.Token == token
}

func (m *mongoBasket) GetResponse(method string) *ResponseConfig {
	if m.ConfigStruct.ResponseConfigs == nil {
		m.ConfigStruct.ResponseConfigs = make(map[string]mongoResponseConfig)
	}

	rc, ok := m.ConfigStruct.ResponseConfigs[method]
	if !ok {
		return nil
	}
	return &ResponseConfig{
		Status:     rc.Status,
		Headers:    rc.Headers,
		Body:       rc.Body,
		IsTemplate: rc.IsTemplate,
	}
}

func (m *mongoBasket) SetResponse(method string, response ResponseConfig) {
	mr := mongoResponseConfig{
		Status:     response.Status,
		Headers:    response.Headers,
		Body:       response.Body,
		IsTemplate: response.IsTemplate,
	}
	_, err := m.db.getBasketCollection().UpdateOne(
		context.Background(),
		bson.M{"name": m.Name},
		bson.M{"$set": bson.M{fmt.Sprintf("config.responseConfigs.%s", method): mr}},
	)
	if err != nil {
		log.Printf("[error] failed to update response config for method '%s' in basket '%s': %s", method, m.Name, err)
	}
}

func (m *mongoBasket) Add(req *http.Request) *RequestData {
	reqData := ToRequestData(req)
	mr := mongoRequestData{
		Basket:        m.Name,
		ID:            primitive.NewObjectID(),
		Header:        reqData.Header,
		ContentLength: reqData.ContentLength,
		Body:          reqData.Body,
		Method:        reqData.Method,
		Path:          reqData.Path,
		Query:         reqData.Query,
	}
	_, err := m.db.getRequestsCollection().InsertOne(context.Background(), mr)
	if err != nil {
		log.Printf("[error] failed to insert request into database: %s", err)
		return reqData
	}
	_, err = m.db.getBasketCollection().UpdateOne(
		context.Background(),
		bson.M{"name": m.Name},
		bson.M{"$inc": bson.M{"requestCount": 1}},
	)
	if err != nil {
		log.Printf("[error] failed to update request count for basket '%s' in database: %s", m.Name, err)
	}

	if m.Size() > m.Config().Capacity {
		overLimit, _ := m.db.getRequests(bson.M{"basket": m.Name}, 1, m.Config().Capacity)
		if overLimit != nil {
			_, _ = m.db.getRequestsCollection().DeleteMany(context.Background(), bson.M{"basket": m.Name, "_id": bson.M{"$lte": overLimit[0].ID}})
		}
	}
	return reqData
}

func (m *mongoBasket) Clear() {
	if m == nil {
		return
	}
	_, err := m.db.getRequestsCollection().DeleteMany(context.Background(), bson.M{"basket": m.Name})
	if err != nil {
		log.Printf("[error] failed to clear basket '%s' in database: %s", m.Name, err)
	}
}

func (m *mongoBasket) Size() int {
	c, _ := m.db.getRequestsCollection().CountDocuments(context.Background(), bson.M{"basket": m.Name})
	return int(c)
}

func (m *mongoBasket) GetRequests(max int, skip int) RequestsPage {
	page := RequestsPage{make([]*RequestData, 0, max), m.Size(), int(m.RequestCount), false}

	if max > 0 {
		requests, _ := m.db.getRequests(bson.M{"basket": m.Name}, max, skip)
		for _, request := range requests {
			page.Requests = append(page.Requests, &RequestData{
				Date:          request.ID.Timestamp().UnixNano() / toMs,
				Header:        request.Header,
				ContentLength: request.ContentLength,
				Body:          request.Body,
				Method:        request.Method,
				Path:          request.Path,
				Query:         request.Query,
			})
		}

	}

	page.HasMore = page.Count > skip+max
	return page
}

func (m *mongoBasket) FindRequests(query string, in string, max int, skip int) RequestsQueryPage {
	//TODO implement me
	panic("implement me")
}

func (m *mongoDatabase) getRequests(filter bson.M, max int, skip int) ([]mongoRequestData, int) {
	r := make([]mongoRequestData, 0, max)
	cur, err := m.getRequestsCollection().Find(
		context.Background(),
		filter,
		options.Find().SetSort(bson.M{"_id": -1}),
		options.Find().SetLimit(int64(max)),
		options.Find().SetSkip(int64(skip)),
	)
	if err != nil {
		log.Printf("[error] failed to get basket names from database: %s", err)
		return nil, 0
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		var result mongoRequestData
		err := cur.Decode(&result)
		if err != nil {
			log.Printf("[error] failed to decode basket name: %s", err)
			return nil, 0
		}
		r = append(r, result)
	}

	count, _ := m.getBasketCollection().CountDocuments(context.Background(), filter)

	return r, int(count)
}

func (m *mongoDatabase) Create(name string, config BasketConfig) (BasketAuth, error) {
	auth := BasketAuth{}
	token, err := GenerateToken()
	if err != nil {
		return auth, fmt.Errorf("failed to generate token: %s", err)
	}

	b := mongoBasket{
		Name:         name,
		RequestCount: 0,
		Token:        token,
		ConfigStruct: mongoBasketConfig{
			ForwardURL:      config.ForwardURL,
			ProxyResponse:   config.ProxyResponse,
			InsecureTLS:     config.InsecureTLS,
			ExpandPath:      config.ExpandPath,
			Capacity:        config.Capacity,
			ResponseConfigs: make(map[string]mongoResponseConfig),
		},
	}
	_, err = m.getBasketCollection().InsertOne(context.Background(), b)
	auth.Token = token
	return auth, err
}

func NewMongoDatabase(connection string) BasketsDatabase {
	connectionUrl, err := url.ParseRequestURI(connection)
	if err != nil {
		log.Printf("[error] Cannot parse connection string: %s - %s", connection, err)
		return nil
	}
	database := connectionUrl.Query().Get("database")
	if database == "" {
		log.Printf("[error] Database name is not specified in connection string: %s", connection)
		return nil
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(connection))
	if err != nil {
		log.Printf("[error] failed to open database connection: %s - %s", connection, err)
		return nil
	}

	if err := client.Ping(context.Background(), nil); err != nil {
		log.Printf("[error] database connection is not alive: %s - %s", connection, err)
		return nil
	}
	log.Printf("[info] connected to MongoDB database")

	m := mongoDatabase{
		db: client.Database(database),
	}
	_, err = m.getBasketCollection().Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys:    bson.M{"name": 1},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		log.Printf("[error] failed to create unique index on 'name': %s", err)
	} else {
		log.Printf("[info] created unique index on 'name' field")
	}
	_, err = m.getRequestsCollection().Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys: bson.M{"basket": 1},
	})
	if err != nil {
		log.Printf("[error] failed to create index on 'name': %s", err)
	} else {
		log.Printf("[info] created index on 'name' field")
	}

	return &m
}

func (m *mongoDatabase) getBasketCollection() *mongo.Collection {
	return m.db.Collection("baskets")
}

func (m *mongoDatabase) getRequestsCollection() *mongo.Collection {
	return m.db.Collection("requests")
}

func (m *mongoDatabase) Get(name string) Basket {
	var mb mongoBasket
	err := m.getBasketCollection().FindOne(context.Background(), bson.M{"name": name}).Decode(&mb)
	if err != nil {
		log.Printf("[error] failed to get basket '%s' from database: %s", name, err)
		return nil
	}
	mb.db = m

	return &mb
}

func (m *mongoDatabase) Delete(name string) {
	m.Get(name).Clear()
	_, err := m.getBasketCollection().DeleteOne(context.Background(), bson.M{"name": name})
	if err != nil {
		log.Printf("[error] failed to delete basket '%s' from database: %s", name, err)
	}
}

func (m *mongoDatabase) Size() int {
	c, _ := m.getBasketCollection().CountDocuments(context.Background(), bson.M{})
	return int(c)
}

func (m *mongoDatabase) GetNames(max int, skip int) BasketNamesPage {
	page := BasketNamesPage{make([]string, 0, max), 0, false}

	page.Names, _, page.Count = m.getNamesAndRequestCount(bson.M{}, max, skip, bson.M{"name": 1})
	page.HasMore = page.Count > skip+max

	return page
}

func (m *mongoDatabase) getNamesAndRequestCount(filter bson.M, max int, skip int, sort bson.M) ([]string, []int, int) {
	r := make([]string, 0, max)
	rc := make([]int, 0, max)
	cur, err := m.getBasketCollection().Find(
		context.Background(),
		filter,
		options.Find().SetProjection(bson.M{"name": 1, "requestCount": 1}),
		options.Find().SetSort(sort),
		options.Find().SetLimit(int64(max)),
		options.Find().SetSkip(int64(skip)),
	)
	if err != nil {
		log.Printf("[error] failed to get basket names from database: %s", err)
		return nil, nil, 0
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		var result mongoBasket
		err := cur.Decode(&result)
		if err != nil {
			log.Printf("[error] failed to decode basket name: %s", err)
			return nil, nil, 0
		}
		r = append(r, result.Name)
		rc = append(rc, int(result.RequestCount))
	}

	count, _ := m.getBasketCollection().CountDocuments(context.Background(), filter)

	return r, rc, int(count)
}

func (m *mongoDatabase) FindNames(query string, max int, skip int) BasketNamesQueryPage {
	page := BasketNamesQueryPage{make([]string, 0, max), false}

	filter := bson.M{"name": bson.M{"$regex": ".*" + regexp.QuoteMeta(query) + ".*", "$options": "i"}}

	var count int
	page.Names, _, count = m.getNamesAndRequestCount(filter, max, skip, bson.M{"name": 1})
	page.HasMore = count > skip+max

	return page
}

func (m *mongoDatabase) GetStats(max int) DatabaseStats {
	stats := DatabaseStats{}

	stats.BasketsCount = m.Size()
	_, _, stats.EmptyBasketsCount = m.getNamesAndRequestCount(bson.M{"requestCount": 0}, 0, 0, bson.M{"_id": -1})
	rc, _ := m.getRequestsCollection().CountDocuments(context.Background(), bson.M{})
	stats.RequestsCount = int(rc)

	cur, _ := m.getBasketCollection().Aggregate(
		context.Background(),
		bson.A{
			bson.M{"$group": bson.M{"_id": nil, "total": bson.M{"$sum": "$requestCount"}, "max": bson.M{"$max": "$requestCount"}}},
		})
	cur.Next(context.Background())
	stat := struct {
		Total int `bson:"total"`
		Max   int `bson:"max"`
	}{}
	_ = cur.Decode(&stat)
	stats.RequestsTotalCount = stat.Total
	stats.MaxBasketSize = stat.Max

	sizeNames, sizeRequests, _ := m.getNamesAndRequestCount(bson.M{}, max, 0, bson.M{"requestCount": -1})
	stats.TopBasketsBySize = make([]*BasketInfo, 0, len(sizeNames))
	for i, name := range sizeNames {
		bi := BasketInfo{Name: name, RequestsTotalCount: sizeRequests[i]}
		basket := m.Get(name)
		bi.RequestsCount = basket.Size()
		if bi.RequestsCount > 0 {
			bi.LastRequestDate = basket.GetRequests(1, 0).Requests[0].Date
		}
		stats.TopBasketsBySize = append(stats.TopBasketsBySize, &bi)
	}
	dateNames, dateRequests, _ := m.getNamesAndRequestCount(bson.M{}, max, 0, bson.M{"_id": -1})
	stats.TopBasketsByDate = make([]*BasketInfo, 0)
	for i, name := range dateNames {
		bi := BasketInfo{Name: name, RequestsTotalCount: dateRequests[i]}
		basket := m.Get(name)
		bi.RequestsCount = basket.Size()
		if bi.RequestsCount > 0 {
			bi.LastRequestDate = basket.GetRequests(1, 0).Requests[0].Date
		}
		stats.TopBasketsByDate = append(stats.TopBasketsByDate, &bi)
	}

	stats.UpdateAvarage()
	return stats
}

func (m *mongoDatabase) Release() {
	log.Printf("[info] closing Mongo database, releasing any open resources")
	_ = m.db.Client().Disconnect(context.Background())
}
