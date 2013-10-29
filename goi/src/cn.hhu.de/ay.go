package main

import "bufio"
import "flag"
import "fmt"
import "io"
import "log"
import "os"
import "os/exec"


type Request struct {
	ip string
	path string
}

func uncompressXz(r io.Reader) io.ReadCloser {
    rpipe, wpipe := io.Pipe()

    cmd := exec.Command("xz", "--decompress", "--stdout")
    cmd.Stdin = r
    cmd.Stdout = wpipe

    go func() {
        err := cmd.Run()
        wpipe.CloseWithError(err)
    }()

    return rpipe
}


func readFile(fn string) chan *Request {
	file, err := os.Open(fn)
	if (err != nil) {
		log.Fatal(err)
	}
	// TODO check file format

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
		if (scanner.Err() != nil) {
			log.Fatal(scanner.Err())
		}
		file.Close()
		close(output)
	}()
	
	return output
}

func readFiles(filenames []string) chan *Request {
	output := make(chan *Request)

	go func() {
		for _, filename := range filenames {
			for r := range readFile(filename) {
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
		for r := range readFiles(positional_args[1:]) {
			fmt.Printf("%s\n", r.path)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown action %s\n", action)
		os.Exit(2)
	}
}
