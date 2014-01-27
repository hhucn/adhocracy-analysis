package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"./counter"
)

// HHU-specific analysis

var INTENSITIES []string = []string{"intensiv", "gering", "besucht", "keine"}

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
		out.Write([]string{})
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
		out.Write([]string{})
	}
	out.Flush()
}


func _participationStats_badges_print_stats(label string, stats *counter.Counter, valOrder []string) {
	count := stats.Sum()
	fmt.Printf("%s(%d): ", label, count)
	first := true
	for _, k := range valOrder {
		if (first) {
			first = false
		} else {
			fmt.Printf(", ")
		}
		fmt.Printf("%s: %d (%d %%)", k, stats.Get(k), (100.0 * stats.Get(k) / count))
	}
	fmt.Println()
}


func participationStats_badges(db *sql.DB, settings Settings, interestingBadges []string) {
	allActivity := make([]*UserActivity, 0)
	for ua := range getUserActivity(db) {
		allActivity = append(allActivity, ua)
	}

	badge_stats := make(map[string]*counter.Counter)
	for _, ib := range interestingBadges {
		badge_stats[ib] = counter.NewCounter()
	}
	stats := counter.NewCounter()
	for _, activity := range allActivity {
		beteiligungStr := classifyUser(activity)
		stats.Count(beteiligungStr)

		badges := getUserBadges(db, activity.user.id)
		for badge, _ := range badges {
			if is_in(badge, interestingBadges) {
				badge_stats[badge].Count(beteiligungStr)
			}
		}
	}

	_participationStats_badges_print_stats("Alle", stats, INTENSITIES)
	for _, b := range interestingBadges {
		_participationStats_badges_print_stats(b, badge_stats[b], INTENSITIES)
	}
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

type tobias_csvWriter struct {
	w *bufio.Writer
}

func tobias_csvWriter_new(w *bufio.Writer) *tobias_csvWriter {
	return &tobias_csvWriter{w}
}

func (tw *tobias_csvWriter) Write(row []string) (err error) {
	for i, cell := range row {
		if i > 0 {
			if _, err = tw.w.WriteString(","); err != nil {
				return
			}
		}

		encoded := "\"" + strings.Replace(cell, "\"", "\"\"", -1) + "\""
		if _, err = tw.w.WriteString(encoded); err != nil {
			return
		}
	}
	tw.w.WriteString("\n")
	return
}

func (tw *tobias_csvWriter) Flush() (err error) {
	return tw.w.Flush()
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
	bwriter := bufio.NewWriter(os.Stdout)
	writer := tobias_csvWriter_new(bwriter)
	headers := []string{"timestamp", "user ID", "questionnaire type", "result"}
	for _, k := range allKeys {
		headers = append(headers, k)
	}
	writer.Write(headers)

	for _, p := range polls {
		row := []string{strconv.FormatInt(p.Time, 10), p.UserId, p.QType, p.Result}
		for _, k := range allKeys {
			a := strings.Replace(p.Answers[k], "\r\n", "\n", -1)
			a = strings.Replace(a, "\n", "<br>", -1)
			row = append(row, a)
		}
		writer.Write(row)
	}
	writer.Flush()
}
