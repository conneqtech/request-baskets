package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"
)

const DbTypeSQL = "sql"
var sqlSchema = []string{
	`CREATE TABLE rb_baskets (
		basket_name varchar(250) PRIMARY KEY,
		token varchar(100) NOT NULL,
		capacity integer NOT NULL,
		forward_url text NOT NULL,
		insecure_tls boolean NOT NULL,
		expand_path boolean NOT NULL,
		requests_count integer NOT NULL DEFAULT 0,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE rb_responses (
		basket_name varchar(250) REFERENCES rb_baskets (basket_name) ON DELETE CASCADE,
		http_method varchar(20) NOT NULL,
		response text NOT NULL,
		PRIMARY KEY (basket_name, http_method)
	)`,
	`CREATE TABLE rb_requests (
		basket_name varchar(250) REFERENCES rb_baskets (basket_name) ON DELETE CASCADE,
		request text NOT NULL,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE INDEX rb_requests_name_time_index ON rb_requests (basket_name, created_at)`,
	`CREATE TABLE rb_version (
		version integer NOT NULL
	)`,
	`INSERT INTO rb_version (version) VALUES (1)`}

/// Basket interface ///
type sqlBasket struct {
	db *sql.DB
	dbType string // postgresql, mysql, oracle, etc.
	name string
}

func (basket *sqlBasket) getInt(sql string, defaultValue int) int {
	var value int
	if err := basket.db.QueryRow(sql, basket.name).Scan(&value); err != nil {
		log.Printf("[error] failed to get counter info about basket: %s - %s", basket.name, err)
		return defaultValue
	} else {
		return value
	}
}

func (basket *sqlBasket) applyLimit(capacity int) {
	// keep the number of requests up to specified capacity
	size := basket.Size()

	if size > capacity {
		// Note: 'ctid' is PostgreSQL specific
		// see example for MySQL here: https://stackoverflow.com/questions/5170546
		_, err := basket.db.Exec("DELETE FROM rb_requests WHERE ctid IN (SELECT ctid FROM rb_requests WHERE basket_name = $1 ORDER BY created_at LIMIT $2)",
			basket.name, size - capacity)
		if err != nil {
			log.Printf("[error] failed to shrink collected requests: %s - %s", basket.name, err)
		}
	}
}

func (basket *sqlBasket) Config() BasketConfig {
	config := BasketConfig{}

	err := basket.db.QueryRow("SELECT capacity, forward_url, insecure_tls, expand_path FROM rb_baskets WHERE basket_name = $1",
		basket.name).Scan(&config.Capacity, &config.ForwardURL, &config.InsecureTLS, &config.ExpandPath);
	if err != nil {
		log.Printf("[error] failed to get basket config: %s - %s", basket.name, err)
	}

	return config
}

func (basket *sqlBasket) Update(config BasketConfig) {
	_, err := basket.db.Exec("UPDATE rb_baskets SET capacity = $1, forward_url = $2, insecure_tls = $3, expand_path = $4 WHERE basket_name = $5",
		config.Capacity, config.ForwardURL, config.InsecureTLS, config.ExpandPath, basket.name)
	if err != nil {
		log.Printf("[error] failed to update basket config: %s - %s", basket.name, err)
	} else {
		// apply new basket limits
		basket.applyLimit(config.Capacity)
	}
}

func (basket *sqlBasket) Authorize(token string) bool {
	var found int

	err := basket.db.QueryRow("SELECT COUNT(*) FROM rb_baskets WHERE basket_name = $1 AND token = $2",
		basket.name, token).Scan(&found)
	if err != nil {
		log.Printf("[error] failed authorize access to basket: %s - %s", basket.name, err)
		return false
	}

	return found > 0
}

func (basket *sqlBasket) GetResponse(method string) *ResponseConfig {
	var resp string

	err := basket.db.QueryRow("SELECT response FROM rb_responses WHERE basket_name = $1 AND http_method = $2",
		basket.name, method).Scan(&resp)
	if err == sql.ErrNoRows {
		// no response for this basket + HTTP method
		return nil
	} else if err != nil {
		log.Printf("[error] failed to get response for HTTP %s method of basket: %s - %s", method, basket.name, err)
		return nil
	}

	response := new(ResponseConfig)
	if err := json.Unmarshal([]byte(resp), response); err != nil {
		log.Printf("[error] failed to parse response for HTTP %s method of basket: %s - %s", method, basket.name, err)
		return nil
	}

	return response
}

func (basket *sqlBasket) SetResponse(method string, response ResponseConfig) {
	if respb, err := json.Marshal(response); err == nil {
		resp := string(respb)
		_, err = basket.db.Exec("INSERT INTO rb_responses (basket_name, http_method, response) VALUES ($1, $2, $3) ON CONFLICT (basket_name, http_method) DO UPDATE SET response = $4",
			basket.name, method, resp, resp)

		if err != nil {
			log.Printf("[error] failed to update response for HTTP %s method of basket: %s - %s", method, basket.name, err)
		}
	}
}

func (basket *sqlBasket) Add(req *http.Request) *RequestData {
	data := ToRequestData(req)
	if datab, err := json.Marshal(data); err == nil {
		_, err = basket.db.Exec("INSERT INTO rb_requests (basket_name, request) VALUES ($1, $2)", basket.name, string(datab))
		if err != nil {
			log.Printf("[error] failed to collect incoming HTTP request in basket: %s - %s", basket.name, err)
		} else {
			// update global counter
			_, err = basket.db.Exec("UPDATE rb_baskets SET requests_count = requests_count + 1 WHERE basket_name = $1", basket.name)
			if err != nil {
				log.Printf("[error] failed to update requests counter of basket: %s - %s", basket.name, err)
			}
			// apply limit if necessary
			// TODO: replace 200 with serverConfig.InitCapacity
			basket.applyLimit(basket.getInt("SELECT capacity FROM rb_baskets WHERE basket_name = $1", 200))
		}
	}

	return data
}

func (basket *sqlBasket) Clear() {
	if _, err := basket.db.Exec("DELETE FROM rb_requests WHERE basket_name = $1", basket.name); err != nil {
		log.Printf("[error] failed to delete collected requests in basket: %s - %s", basket.name, err)
	}
}

func (basket *sqlBasket) Size() int {
	return basket.getInt("SELECT COUNT(*) FROM rb_requests WHERE basket_name = $1", 0)
}

func (basket *sqlBasket) GetRequests(max int, skip int) RequestsPage {
	page := RequestsPage{make([]*RequestData, 0, max), basket.Size(),
		basket.getInt("SELECT requests_count FROM rb_baskets WHERE basket_name = $1", 0), false}

	if max > 0 {
		requests, err := basket.db.Query("SELECT request FROM rb_requests WHERE basket_name = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
			basket.name, max + 1, skip)
		if err != nil {
			log.Printf("[error] failed to get requests of basket: %s - %s", basket.name, err)
			return page
		}
		defer requests.Close()

		var req string
		for len(page.Requests) < max && requests.Next() {
			if err = requests.Scan(&req); err != nil {
				log.Printf("[error] failed to get request of basket: %s - %s", basket.name, err)
			} else {
				request := new(RequestData)
				if err = json.Unmarshal([]byte(req), request); err != nil {
					log.Printf("[error] failed to parse HTTP request data in basket: %s - %s", basket.name, err)
				} else {
					page.Requests = append(page.Requests, request)
				}
			}
		}

		page.HasMore = requests.Next()
	} else {
		page.HasMore = page.Count > skip
	}

	return page
}

func (basket *sqlBasket) FindRequests(query string, in string, max int, skip int) RequestsQueryPage {
	page := RequestsQueryPage{make([]*RequestData, 0, max), false}
	if max > 0 {
		requests, err := basket.db.Query("SELECT request FROM rb_requests WHERE basket_name = $1 ORDER BY created_at DESC", basket.name)
		if err != nil {
			log.Printf("[error] failed to find requests of basket: %s - %s", basket.name, err)
			return page
		}
		defer requests.Close()

		skipped := 0
		var req string
		for len(page.Requests) < max && requests.Next() {
			if err = requests.Scan(&req); err != nil {
				log.Printf("[error] failed to get request of basket: %s - %s", basket.name, err)
			} else {
				request := new(RequestData)
				if err = json.Unmarshal([]byte(req), request); err != nil {
					log.Printf("[error] failed to parse HTTP request data in basket: %s - %s", basket.name, err)
				} else {
					// filter
					if request.Matches(query, in) {
						if skipped < skip {
							skipped++
						} else {
							page.Requests = append(page.Requests, request)
						}
					}
				}
			}
		}
		page.HasMore = requests.Next()
	} else {
		page.HasMore = true
	}

	return page
}

/// BasketsDatabase interface ///

type sqlDatabase struct {
	db *sql.DB
	dbType string // postgresql, mysql, oracle, etc.
}

func (sdb *sqlDatabase) Create(name string, config BasketConfig) (BasketAuth, error) {
	auth := BasketAuth{}
	token, err := GenerateToken()
	if err != nil {
		return auth, fmt.Errorf("Failed to generate token: %s", err)
	}

	basket, err := sdb.db.Exec("INSERT INTO rb_baskets (basket_name, token, capacity, forward_url, insecure_tls, expand_path) VALUES($1, $2, $3, $4, $5, $6)",
		name, token, config.Capacity, config.ForwardURL, config.InsecureTLS, config.ExpandPath)
	if err != nil {
		return auth, fmt.Errorf("Failed to create basket: %s - %s", name, err)
	}

	if _, err := basket.RowsAffected(); err != nil {
		return auth, err
	}

	auth.Token = token
	return auth, nil
}

func (sdb *sqlDatabase) Get(name string) Basket {
	var basket_name string
	err := sdb.db.QueryRow("SELECT basket_name FROM rb_baskets WHERE basket_name = $1", name).Scan(&basket_name)

	if err == sql.ErrNoRows {
		log.Printf("[warn] no basket found: %s", name)
		return nil
	} else if err != nil {
		log.Printf("[error] failed to get basket: %s - %s", name, err)
		return nil
	}

	return &sqlBasket{sdb.db, sdb.dbType, name}
}

func (sdb *sqlDatabase) Delete(name string) {
	if _, err := sdb.db.Exec("DELETE FROM rb_baskets WHERE basket_name = $1", name); err != nil {
		log.Printf("[error] failed to delete basket: %s - %s", name, err)
	}
}

func (sdb *sqlDatabase) Size() int {
	var size int
	if err := sdb.db.QueryRow("SELECT COUNT(*) FROM rb_baskets").Scan(&size); err != nil {
		log.Printf("[error] failed to get the total number of baskets: %s", err)
		return 0
	} else {
		return size
	}
}

func (sdb *sqlDatabase) GetNames(max int, skip int) BasketNamesPage {
	page := BasketNamesPage{make([]string, 0, max), sdb.Size(), false}

	names, err := sdb.db.Query("SELECT basket_name FROM rb_baskets ORDER BY basket_name LIMIT $1 OFFSET $2", max + 1, skip)
	if err != nil {
		log.Printf("[error] failed to get basket names: %s", err)
		return page
	}
	defer names.Close()

	var name string
	for len(page.Names) < max && names.Next() {
		if err = names.Scan(&name); err != nil {
			log.Printf("[error] failed to get basket name: %s", err)
		} else {
			page.Names = append(page.Names, name)
		}
	}

	page.HasMore = names.Next()

	return page
}

func (sdb *sqlDatabase) FindNames(query string, max int, skip int) BasketNamesQueryPage {
	page := BasketNamesQueryPage{make([]string, 0, max), false}

	names, err := sdb.db.Query("SELECT basket_name FROM rb_baskets WHERE basket_name LIKE $1 ORDER BY basket_name LIMIT $2 OFFSET $3",
		"%" + query + "%", max + 1, skip)
	if err != nil {
		log.Printf("[error] failed to find basket names: %s", err)
		return page
	}
	defer names.Close()

	var name string
	for len(page.Names) < max && names.Next() {
		if err = names.Scan(&name); err != nil {
			log.Printf("[error] failed to get basket name: %s", err)
		} else {
			page.Names = append(page.Names, name)
		}
	}

	page.HasMore = names.Next()

	return page
}

func (sdb *sqlDatabase) Release() {
	log.Printf("[info] closing SQL database, releasing any open resources")
	sdb.db.Close()
}

// NewSQLDatabase creates an instance of Baskets Database backed with SQL DB
func NewSQLDatabase(connection string) BasketsDatabase {
	log.Print("[info] using SQL database to store baskets")

	dbType := "postgres" // TODO: determine from connection string
	log.Printf("[info] SQL database type: %s", dbType)

	db, err := sql.Open(dbType, connection)
	if err != nil {
		log.Printf("[error] failed to open database connection: %s - %s", connection, err)
		return nil
	}

	if err = db.Ping(); err != nil {
		log.Printf("[error] database connection is not alive: %s - %s", connection, err)
	} else if err = initSchema(db); err != nil {
		log.Printf("[error] failed to initialize SQL schema: %s", err)
	} else {
		return &sqlDatabase{db, dbType}
	}

	db.Close()
	return nil
}

func initSchema(db *sql.DB) error {
	switch version := getSchemaVersion(db); version {
	case 0:
		return createSchema(db)
	case 1:
		log.Printf("[info] database schema already exists, version: %v", version)
		return nil
	default:
		return fmt.Errorf("unknown database schema version: %v", version)
	}
}

func getSchemaVersion(db *sql.DB) int {
	var version int
	if err := db.QueryRow("SELECT version FROM rb_version").Scan(&version); err != nil {
		return 0
	} else {
		return version
	}
}

func createSchema(db *sql.DB) error {
	log.Printf("[info] creating database schema")
	for idx, stmt := range sqlSchema {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("error in SQL statement #%v - %s", idx, err)
		}
	}
	log.Printf("[info] database is created, version: %v", getSchemaVersion(db))
	return nil
}
