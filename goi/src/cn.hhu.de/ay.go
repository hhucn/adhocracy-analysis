package main

import (
	"./counter"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"regexp"
	"time"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
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

// Creates an analysis table and returns whether the table was already present and its final name.
func makeTable(db *sql.DB, name string, definitions string) (bool, string) {
	tableName := "analysis_" + name

	_, err := db.Exec("select 1 from " + tableName + " limit 0")
	if err == nil {
		return true, tableName
	}

	sql := "CREATE TABLE " + tableName + " (" + definitions + ")";
	_, createErr := db.Exec(sql)
	if createErr != nil {
		panic(createErr.Error())
	}

	return false, tableName
}

func makeNewTable(db *sql.DB, name string, definitions string) string {
	exists, tname := makeTable(db, name, definitions)
	if exists {
		_, err := db.Exec("DROP TABLE " + tname)
		assertMust(err, "err != nil")
		exists, tname2 := makeTable(db, name, definitions)
		assertMsg(tname == tname2, "equal table names")
		assertMsg(! exists, "! exists")
	}
	return tname
}

func allRequests(db *sql.DB, settings Settings) <-chan Request {
	c := make(chan Request, 1000)
	go func() {
		rows, err := db.Query(
			"SELECT id, access_time, ip_address, request_url, cookies, user_agent, referer " +
			"FROM requestlog WHERE access_time >= ? and access_time <= ? " +
			"AND user_agent NOT LIKE 'ApacheBench/%' AND user_agent NOT LIKE 'Pingdom.com%' AND user_agent NOT LIKE '%bot%'",
			settings.StartDate, settings.EndDate)
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

type Settings struct {
	Dsn string
	StartDate string
	EndDate string
}

func readSettings(fn string) Settings {
	jsonCode, err := ioutil.ReadFile(fn)
	if err != nil {
		panic(err.Error())
	}
 
	var settings Settings
	jerr := json.Unmarshal(jsonCode, &settings)
	if jerr != nil {
		panic(jerr.Error())
	}
	return settings
}

// Map from user name to user id
func getUserIdMap(db *sql.DB) map[string]int { 
	rows, err := db.Query("SELECT id, email, user_name FROM user")
	if err != nil {
		panic(err.Error())
	}

	res := map[string]int{}
	for rows.Next() {
		var id int
		var name string
		var email string

		err = rows.Scan(&id, &email, &name)
		if err != nil {
			panic(err.Error())
		}
		res[name] = id
		res[email] = id
	}
	return res
}

type User struct {
	id int
	name string
	email string
}

func getAllUsers(db *sql.DB) []User {
	rows, err := db.Query("SELECT id, email, display_name FROM user")
	if err != nil {
		panic(err.Error())
	}

	res := []User{}
	for rows.Next() {
		var u User
		err = rows.Scan(&u.id, &u.email, &u.name)
		if err != nil {
			panic(err.Error())
		}
		res = append(res, u)
	}
	return res
}

type UserActivity struct {
	user User
	CommentCount int
	ProposalCount int
	VoteCount int
}
func getUserActivity(db *sql.DB) map[int]*UserActivity {
	res := make(map[int]*UserActivity)
	for _, u := range getAllUsers(db) {
		ua := new(UserActivity)
		ua.user = u
		res[u.id] = ua
	}

	rows, err := db.Query(`
		select user.id as user_id, count(comment.id) as comment_count
		from user, comment
		where
			comment.delete_time IS NULL and
			user.id = comment.creator_id
		group by user.id`)
	if err != nil {
		panic(err.Error())
	}
	for rows.Next() {
		var userId int
		var count int
		err = rows.Scan(&userId, &count)
		if err != nil {
			panic(err.Error())
		}
		res[userId].CommentCount = count
	}

	rows, err = db.Query(`
		select user.id as user_id, count(delegateable.id) as proposal_count
		from user, delegateable
		where
			delegateable.delete_time IS NULL and
			user.id = delegateable.creator_id and
			delegateable.type = "proposal"
		group by user.id`)
	if err != nil {
		panic(err.Error())
	}
	for rows.Next() {
		var userId int
		var count int
		err = rows.Scan(&userId, &count)
		if err != nil {
			panic(err.Error())
		}
		res[userId].ProposalCount = count
	}

	rows, err = db.Query(`
		select user.id as user_id, count(vote.id) as vote_count
		from user, vote
		where
			user.id = vote.user_id
		group by user.id`)
	if err != nil {
		panic(err.Error())
	}
	for rows.Next() {
		var userId int
		var count int
		err = rows.Scan(&userId, &count)
		if err != nil {
			panic(err.Error())
		}
		res[userId].VoteCount = count
	}

	return res
}

func tagRequestUsers(db *sql.DB, settings Settings) {
	table := makeNewTable(db, "request_users", "request_id INT, user_id INT")
	userIds := getUserIdMap(db)

	stmtIns, err := db.Prepare("INSERT INTO " + table + " VALUES(?, ?)")
	if err != nil {
		panic(err.Error())
	}
	defer stmtIns.Close()
	welcome_re := regexp.MustCompile("^/(?:i/[a-z_]+/)?welcome/([A-Za-z0-9_.-]+)")
	cookie_re := regexp.MustCompile("adhocracy_login=[0-9a-f]{40}([^!]+)!")

	tagUser := func(requestId int, userName string) {
		userId, found := userIds[userName]
		if found {
			stmtIns.Exec(requestId, userId)
		} else {
			fmt.Printf("Could not find user %s\n", userName)
		}
	}

	for req := range allRequests(db, settings) {
		// Welcome URL
		m := welcome_re.FindStringSubmatch(req.request_url)
		if len(m) > 0 {
			tagUser(req.id, m[1])
			continue
		}

		// Cookie
		m = cookie_re.FindStringSubmatch(req.cookies)
		if len(m) > 0 {
			tagUser(req.id, m[1])
			continue
		}
	}

}

func listUserAgents(db *sql.DB, settings Settings) {
	uaCounts := counter.NewCounter()
	for req := range allRequests(db, settings) {
		uaCounts.Count(req.user_agent)
	}
	for _, it := range uaCounts.MostCommon() {
		fmt.Printf("%8d %s\n", it.Value, it.Key)
	}
}

func classifyUsers(db *sql.DB, settings Settings) {
	for _, activity := range getUserActivity(db) {
		fmt.Println(activity)
		// TODO csv
	}
}

func main() {
	cfgFile := flag.String("config", ".ayconfig", "Configuration file to read from")
	fulldsn := flag.String("dsn", "(from config)", "The database connection string")
	flag.Parse()

	settings := readSettings(*cfgFile)
	if *fulldsn == "(from config)" {
		*fulldsn = settings.Dsn
	}

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
		tagRequestUsers(db, settings)
	case "listUserAgents":
		listUserAgents(db, settings)
	case "classifyUsers":
		classifyUsers(db, settings)
	case "listUserIds":
		for name, id := range getUserIdMap(db) {
			fmt.Printf("%s : %d\n", name, id)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown action %s\n", action)
		os.Exit(2)
	}
}
