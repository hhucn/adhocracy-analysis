package main

import (
	"./counter"
	"database/sql"
	"flag"
	"fmt"
	"os"
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
	return allRequestsWhere(
		db,
		"(access_time >= ? and access_time <= ?)",
		settings.StartDate, settings.EndDate)
}

func allRequestsWhere(db *sql.DB, where string, args ...interface{}) <-chan Request {
	c := make(chan Request, 1000)
	go func() {
		rows, err := db.Query(
			"SELECT id, access_time, ip_address, request_url, cookies, user_agent, referer " +
			"FROM requestlog " +
			"WHERE " + where + " " +
			"AND user_agent NOT LIKE 'ApacheBench/%' AND user_agent NOT LIKE 'Pingdom.com%' AND user_agent NOT LIKE '%bot%'",
			args...)
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

func fixIPs(db *sql.DB, settings Settings, filename string) {
	// TODO
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
	RequestCount int
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

	rows, err = db.Query(`
		select user.id, count(analysis_request_users.user_id) as request_count
		from user, analysis_request_users
		where
			user.id = analysis_request_users.user_id
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
		res[userId].RequestCount = count
	}

	return res
}

// Returns a set of users with the given badge
func getUserIdsWithBadge(db *sql.DB, badge string) map[int]int {
	rows, err := db.Query(`
		select user_badges.user_id
		from user_badges, badge
		where
			user_badges.badge_id = badge.id and
			badge.title = ?
		`, badge)
	if err != nil {
		panic(err.Error())
	}
	res := make(map[int]int)
	for rows.Next() {
		var userId int
		err = rows.Scan(&userId)
		if err != nil {
			panic(err.Error())
		}
		res[userId] = 1
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
	}
}

func argumentError(msg string) {
	println(msg)
	flag.Usage()
	os.Exit(2)
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
		argumentError("No action specified")
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
	case "fixIPs":
		fixIPs(db, settings, positional_args[1])
	case "tranow_classifyUsers":
		tranow_classifyUsers(db, settings, positional_args[1])
	case "tobias_poll":
		tobias_poll(db, settings)
	case "listUserIds":
		for name, id := range getUserIdMap(db) {
			fmt.Printf("%s : %d\n", name, id)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown action %s\n", action)
		os.Exit(2)
	}
}
