package main

import (
	"database/sql"
	"fmt"
	"strings"
	"regexp"
)

// HHU-specific analysis

func tranow_classifyUsers(db *sql.DB, settings Settings, badge string) {
	usersWithBadge := getUserIdsWithBadge(db, badge)
	for _, activity := range getUserActivity(db) {
		_, hasBadge := usersWithBadge[activity.user.id]
		var badgeStr string
		if hasBadge {
			badgeStr = "Ja"
		} else {
			badgeStr = "Nein"
		}

		var beteiligungStr string
		switch {
		case activity.ProposalCount > 0 || activity.CommentCount > 0:
			beteiligungStr = "intensiv"
		case activity.VoteCount > 0:
			beteiligungStr = "gering"
		case activity.RequestCount > 0:
			beteiligungStr = "besucht"
		default:
			beteiligungStr = "keine"
		}

		fmt.Printf("%s,%s,%s,%s\n",
			activity.user.name, activity.user.email, badgeStr, beteiligungStr)
	}
}

func tobias_poll(db *sql.DB, settings Settings) {
	for r := range allRequestsWhere(db, "request_url LIKE '/static/nb%'") {
		path, _, args := partition(r.request_url, "?")

		match := re_match("^$|^u=$|^.*u=(?:12345|19990)", args)
		if match {
			continue
		}

		var (
			rtype string
			user string
			result string
			answers string
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
				answers = matches[2]
			} else {
				result = matches[1]
				answers = matches[2]
				user = matches[3]
			}

			rtype = "extended"
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

			answers = ""
			rtype = "basic"
		default:
			assertMsg(false, "Unsupported URL " + path)
		}

		fmt.Printf("%d %s %s %s %s\n", r.access_time, user, rtype, result, answers)
	}
}
