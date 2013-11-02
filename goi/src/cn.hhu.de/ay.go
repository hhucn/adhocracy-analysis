package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)


import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
)


type Request struct {
	id int
	access_time int64
	ip_address string
	request_url string
	cookies string
	user_agent string
	referer string
}


func partition(s string, sep string) (string, string, string) {
	parts := strings.SplitN(s, sep, 2)
	if len(parts) == 1 {
		return parts[0], "", ""
	}
	return parts[0], sep, parts[1]
}

func connectDb(fulldsn string) *sql.DB {
	dbtype, _, dsn := partition(fulldsn, ":")
	db, err := sql.Open(dbtype, dsn)
	if err != nil {
		panic(err.Error())
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	return db
}

// Creates an analysis table and returns whether the table was already present and its final name.
func makeTable(name string, definitions string) (bool, string) {
	tableName := "analysis_" + name 
	sql := "CREATE TABLE " + tableName + " " + definitions;
	println("TODO" + sql)

	return false, tableName
}

func allRequests(db *sql.DB) <-chan Request {
	c := make(chan Request, 1000)
	go func() {
		rows, err := db.Query("SELECT id, access_time, ip_address, request_url, cookies, user_agent, referer FROM requestlog")
		if err != nil {
			panic(err.Error())
		}
		for rows.Next() {
			var req Request
			var datestr string
			var user_agent sql.NullString
			var referer sql.NullString
			var cookies sql.NullString

			err = rows.Scan(&req.id, &datestr, &req.ip_address, &req.request_url, &cookies, &user_agent, &referer)
			if err != nil {
				panic(err.Error())
			}
			if user_agent.Valid {
				req.user_agent = user_agent.String
			} else {
				req.user_agent = "(unspecified)"
			}
			if referer.Valid {
				req.referer = referer.String
			} else {
				req.referer = "(none)"
			}
			if cookies.Valid {
				req.cookies = cookies.String
			} else {
				req.cookies = "(none)"
			}
			t, err := time.Parse("2006-01-02 15:04:05", datestr)
			if err != nil {
				panic(err.Error())
			}
			req.access_time = t.Unix()
			c <- req
		}
		close(c)
	} ()

	return c
}

// Map from user name to user id
func getUserIds(db *sql.DB) map[string]int { 
	rows, err := db.Query("SELECT id, user_name FROM user")
	if err != nil {
		panic(err.Error())
	}

	res := map[string]int{}
	for rows.Next() {
		var id int
		var name string

		err = rows.Scan(&id, &name)
		if err != nil {
			panic(err.Error())
		}
		res[name] = id
	}
	return res
}


func tagRequestUsers(db *sql.DB) {
	userIds := getUserIds(db)

	for req := range allRequests(db) {
		fmt.Println(req)
	}


	println(userIds) // TODO do something with them
	/*
	done, tableName := makeTable("request_users", "INT request_id, INT user_id")
	if (done) {
		return
	}
	println(tableName)

	stmtIns, err := db.Prepare("INSERT INTO squareNum VALUES( ?, ? )")
	if err != nil {
		panic(err.Error())
	}
	defer stmtIns.Close()*/

}


func main() {
	fulldsn := flag.String("dsn", "mysql:normsetzung@/normsetzung", "The database connection string")
	flag.Parse()

	positional_args := flag.Args()
	if len(positional_args) < 1 {
		println("No action specified")
		flag.Usage()
		os.Exit(2)
	}
	action := positional_args[0]

	db := connectDb(*fulldsn)
	defer db.Close()

	switch action {
	case "tagRequestUsers":
		tagRequestUsers(db)
	default:
		fmt.Fprintf(os.Stderr, "Unknown action %s\n", action)
		os.Exit(2)
	}
}
