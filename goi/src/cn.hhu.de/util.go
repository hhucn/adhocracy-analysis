package main

import (
	"database/sql"
	"strings"
	"regexp"
)

func assertMsg(b bool, msg string) {
	if !b {
		panic("Assertion failed: " + msg);
	}
}

func assertMust(e error, msg string) {
	if e != nil {
		panic(msg + e.Error())
	}
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

func re_match(pattern string, input string) bool {
	match, err := regexp.MatchString(pattern, input)
	assertMust(err, "Invalid regexp")
	return match
}