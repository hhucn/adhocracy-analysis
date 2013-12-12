package main

import (
	"database/sql"
	"net/url"
	"regexp"
	"strings"
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

func db2int(v sql.NullInt64) int {
	 if (v.Valid) {
	 	return int(v.Int64)
	 } else {
	 	return 0
	 }
}

func parse_urlencoded(raw string) map[string]string {
	fakeUrl := "http://localhost?" + raw
	url, err := url.Parse(fakeUrl)
	assertMust(err, "Invalid URL")
	res := make(map[string]string, 0)
	for key, values := range url.Query() {
		res[key] = values[0]
	}
	return res
}

func first_present(search_in map[string]bool, needles []string) string {
	for _, needle := range needles {
		_, exists := search_in[needle]
		if exists {
			return needle
		}
	}
	return ""
}

func is_in(needle string, haystack []string) bool {
	for _, el := range haystack {
		if needle == el {
			return true
		}
	}
	return false
}

func map_keys(m map[string]interface{}) []string {
	res := make([]string, len(m))
	for k, _ := range m {
		res = append(res, k)
	}
	return res
}
