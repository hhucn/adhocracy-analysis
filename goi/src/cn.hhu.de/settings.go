package main

import (
	"encoding/json"
	"io/ioutil"
)

type Phase struct {
	StartDate string
	EndDate string
}

type Settings struct {
	Dsn string
	StartDate string
	EndDate string
	Phases []Phase
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
