package logmetrics

import (
	"github.com/ActiveState/tail"
	//	"github.com/deckarep/golang-set"
	"log"
	"path/filepath"
	"time"
)

func tailFile(channel_number int, filename string, logGroup *LogGroup) {
	//Recovery setup
	//defer func() {
	//	if r := recover(); r != nil {
	//		log.Printf("Recovering from %s", r)
	//	}
	//}()
	//Number of matches expected = length of the destination table + 1 (stime)
	maxMatches := logGroup.expected_matches + 1

	//os.Seek end of file descriptor
	seekParam := 2
	if logGroup.parse_from_start {
		seekParam = 0
	}

	loc := tail.SeekInfo{0, seekParam}

	tail, err := tail.TailFile(filename, tail.Config{Location: &loc, Follow: true, ReOpen: true, Poll: true})
	if err != nil {
		log.Fatalf("Unable to tail %s: %s", filename, err)
		return
	}
	log.Printf("Tailing %s data to datapool[%s:%d]", filename, logGroup.name, channel_number)

	//FIXME: Bug in ActiveTail can get partial lines
	for line := range tail.Lines {
		if line.Err != nil {
			log.Printf("Tail on %s was lost: %s", filename, err)
			return
		}

		//Test out all the regexp, pick the first one that matches
		match_one := false
		for _, re := range logGroup.re {
			if matches := re.FindStringSubmatch(line.Text); len(matches) == maxMatches {
				//Decide which datapool channel to send the line to
				//split_val := logGroup.workload_split_on + 1

				match_one = true
				logGroup.tail_data[channel_number] <- matches
			}
		}

		if logGroup.fail_regex_warn && !match_one {
			log.Printf("Regexp match failed on %s, expected %d matches: %s", filename, maxMatches, line.Text)
		}
	}

	log.Printf("Finished tailling %s.", filename)
}
func startLogGroup(logGroup *LogGroup, pollInterval int) {
	log.Printf("Filename poller for %s started", logGroup.name)
	log.Printf("Using the following regexp for log group %s: %s", logGroup.name, logGroup.strRegexp)

	rescanFiles := make(chan bool, 1)
	go func() {
		rescanFiles <- true
		for {
			time.Sleep(time.Duration(pollInterval) * time.Second)
			rescanFiles <- true
		}
	}()

	currentFiles := make(map[string]bool)
	channel_number := 0
	for {
		select {
		case <-rescanFiles:
			newFiles := make(map[string]bool)
			for _, glob := range logGroup.globFiles {
				files, err := filepath.Glob(glob)
				if err != nil {
					log.Fatalf("Unable to find files for log group %s: %s", logGroup.name, err)
				}

				for _, v := range files {
					newFiles[v] = true
				}
			}

			//Check only the diff, missing files will automatically be dropped
			//by their goroutine
			for file, _ := range newFiles {
				if ok := currentFiles[file]; ok {
					delete(newFiles, file)
				}
			}

			//Start tailing new files!
			for file, _ := range newFiles {
				go tailFile(channel_number, file, logGroup)
				channel_number = (channel_number + 1) % logGroup.goroutines

				currentFiles[file] = true
			}
		}
	}
}

func StartTails(config *Config) {
	for _, logGroup := range config.logGroups {
		go startLogGroup(logGroup, config.pollInterval)
	}
}
