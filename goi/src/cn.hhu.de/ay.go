package main

import "bufio"
import "flag"
import "fmt"
import "log"
import "os"

import "code.google.com/p/lzma"



type Request struct {
	ip string
	path string
}

func read_file(fn string) chan *Request {
	file, err := os.Open(fn)
	if (err != nil) {
		log.Fatal(err)
	}
	// TODO check file format
	r := lzma.NewReader(file)
	r.Close()

	output := make(chan *Request)
	go func() {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			r := new(Request)
			r.ip = "TODO: ip"
			r.path = line
			output <- r
		}
		close(output)
	}()
	
	return output
}

func read_files(filenames []string) chan *Request {
	output := make(chan *Request)

	go func() {
		for _, filename := range filenames {
			for r := range read_file(filename) {
				output <- r
			}
		}
		close(output)
	}()

	return output
}

func main() {
	flag.Parse()
	positional_args := flag.Args()
	if len(positional_args) < 1 {
		println("Not enough arguments")
		flag.Usage()
		os.Exit(2)
	}
	action := positional_args[0]

	switch action {
	case "listrequests":
		for r := range read_files(positional_args[1:]) {
			fmt.Printf("%s\n", r.path)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown action %s\n", action)
		os.Exit(2)
	}
}
