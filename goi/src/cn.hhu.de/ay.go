package main

import "flag"
import "fmt"
import "os"


import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
)


import "strings"

type Request struct {
	id int
	access_time int
	ip_address string
	url string
	cookes string
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

func allRequests() <- chan Request {
	res := make(chan Request)
	return res
}

// Map from user name to user id
func userIds(db *sql.DB) map[string]int { 
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
	user_ids := userIds(db)

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
