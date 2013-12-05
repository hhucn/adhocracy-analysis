package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// HHU-specific analysis

func classifyUser(activity *UserActivity) string {
	switch {
	case activity.ProposalCount > 0 || activity.CommentCount > 0:
		return "intensiv"
	case activity.VoteCount > 0:
		return "gering"
	case activity.RequestCount > 0:
		return "besucht"
	default:
		return "keine"
	}
}

func tranow_classifyUsers(db *sql.DB, settings Settings, badge string) {
	usersWithBadge := getUserIdsWithBadge(db, badge)
	for activity := range getUserActivity(db) {
		_, hasBadge := usersWithBadge[activity.user.id]
		var badgeStr string
		if hasBadge {
			badgeStr = "Ja"
		} else {
			badgeStr = "Nein"
		}

		beteiligungStr := classifyUser(activity)
		fmt.Printf("%s,%s,%s,%s\n",
			activity.user.name, activity.user.email, badgeStr, beteiligungStr)
	}
}

func tobias_activityPhases(db *sql.DB, settings Settings, interestingBadgesRaw []string) {
	interestingBadges := make([][]string, 0)
	for _, ibr := range interestingBadgesRaw {
		interestingBadges = append(interestingBadges, strings.Split(ibr, ","))
	}
	out := csv.NewWriter(os.Stdout)
	for _, phase := range settings.Phases {
		out.Write([]string{phase.Name})
		headers := []string{"user", "email", "beteiligung"}
		for _, ibr := range interestingBadgesRaw {
			headers = append(headers, ibr)
		}
		out.Write(headers)

		for activity := range getUserActivityByDate(db, phase.StartDate, phase.EndDate) {
			beteiligungStr := classifyUser(activity)
			badges := getUserBadges(db, activity.user.id)

			row := []string{activity.user.name, activity.user.email, beteiligungStr}
			for _, ib := range interestingBadges {
				row = append(row, first_present(badges, ib))
			}
			out.Write(row)
		}
	}
	out.Flush()
}

type TobiasPoll struct {
	Time int64
	UserId string
	QType string
	Result string
	Answers map[string]string
}

func tobias_poll_getdata(db *sql.DB, settings Settings) []TobiasPoll {
	res := make([]TobiasPoll, 0)
	for r := range allRequestsWhere(db, "request_url LIKE '/static/nb%'") {
		path, _, args := partition(r.request_url, "?")

		match := re_match("^$|^u=$|^.*u=(?:12345|19990)", args)
		if match {
			continue
		}

		var (
			qtype string
			user string
			result string
			raw_answers string
		)
		switch {
		case path == "/static/nbthanks.html":
			re := regexp.MustCompile("^result=([^&]+)&(.*)&u=([0-9]+)$")
			matches := re.FindStringSubmatch(args)
			if len(matches) != 4 {
				if r.referer == "(none)" {
					continue  // Some manual tests
				}
				re = regexp.MustCompile(".*[?]u=([0-9]+)$")
				matches = re.FindStringSubmatch(r.referer)
				user = matches[1]

				re = regexp.MustCompile("^result=([^&]+)&(.*)$")
				matches = re.FindStringSubmatch(args)
				assertMsg(len(matches) == 3, "Cannot find 3 args: " + args + "\n")
				result = matches[1]
				raw_answers = matches[2]
			} else {
				result = matches[1]
				raw_answers = matches[2]
				user = matches[3]
			}

			qtype = "extended"
		case strings.HasPrefix(path, "/static/nb"):
			longNames := map[string]string{
			    "z": "satisfied",
			    "w": "waage",
			    "k": "neutral",
			    "u": "dissatisfied",
			}

			re := regexp.MustCompile("u=([0-9]+)$")
			matches := re.FindStringSubmatch(args)
			if (len(matches) != 2) || matches[1] == "1" {
				continue // Random requests
			}
			user = matches[1]

			re = regexp.MustCompile("/static/nb(.)")
			matches = re.FindStringSubmatch(path)
			assertMsg(len(matches) == 2, "Invalid query " + r.request_url)
			short_result := matches[1]
			result = longNames[short_result]

			raw_answers = ""
			qtype = "basic"
		default:
			assertMsg(false, "Unsupported URL " + path)
		}

		answers := parse_urlencoded(raw_answers)
		p := TobiasPoll{r.access_time, user, qtype, result, answers}
		res = append(res, p)
	}
	return res
}

func tobias_poll(db *sql.DB, settings Settings) {
	polls := tobias_poll_getdata(db, settings)
	allKeys_map := make(map[string]bool)
	for _, p := range polls {
		for key, _ := range p.Answers {
			allKeys_map[key] = true
		}
	}

	allKeys := make([]string, 0)
	for key, _ := range allKeys_map {
		allKeys = append(allKeys, key)
	}
	sort.Strings(allKeys)

	// Header row
	writer := csv.NewWriter(os.Stdout)
	headers := []string{"timestamp", "user ID", "questionnaire type", "result"}
	for _, k := range allKeys {
		headers = append(headers, k)
	}
	writer.Write(headers)

	for _, p := range polls {
		row := []string{strconv.FormatInt(p.Time, 10), p.UserId, p.QType, p.Result}
		for _, v := range p.Answers {
			row = append(row, v)
		}
		writer.Write(row)
	}
	writer.Flush()
}
