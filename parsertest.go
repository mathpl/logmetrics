package logmetrics

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

type readStats struct {
	key_pushed  int64
	byte_pushed int64
	last_report time.Time
}

func (f *readStats) inc(data_read int) {
	f.key_pushed++
	f.byte_pushed += int64(data_read)
}

func (f *readStats) getStats() string {
	line_sec := int(f.key_pushed / int64(time.Now().Sub(f.last_report)/time.Second))
	mbyte_sec := float64(f.byte_pushed) / 1024 / 1024 / float64(time.Now().Sub(f.last_report)/time.Second)

	f.key_pushed = 0
	f.byte_pushed = 0
	f.last_report = time.Now()

	return fmt.Sprintf("%d line/s. %.3f Mb/s.",
		line_sec, mbyte_sec)
}

func (f *readStats) isTimeForStats(interval int) bool {
	return (time.Now().Sub(f.last_report) > time.Duration(interval)*time.Second)
}

func parserTest(filename string, logGroup *LogGroup, perfInfo bool) {
	maxMatches := logGroup.expected_matches + 1

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Unable to tail %s: %s", filename, err)
		return
	}

	scanner := bufio.NewScanner(file)

	log.Printf("Parsing %s", filename)

	read_stats := readStats{last_report: time.Now()}
	for scanner.Scan() {
		line := scanner.Text()

		//Test out all the regexp, pick the first one that matches
		match_one := false
		for _, re := range logGroup.re {
			m := re.MatcherString(line, 0)
			matches := buildMatches(line, m)
			if len(matches) == maxMatches {
				match_one = true
			}
		}

		if match_one {
			read_stats.inc(len(line))
		}

		if read_stats.isTimeForStats(5) {
			log.Print(read_stats.getStats())
		}
	}

	log.Printf("Finished tailling %s.", filename)
}

func startLogGroupParserTest(logGroup *LogGroup, perfInfo bool) {

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

	//Start tailing new files!
	for file, _ := range newFiles {
		parserTest(file, logGroup, perfInfo)
	}

}

func StartParserTest(config *Config, selectedLogGroup string, perfInfo bool) {
	for logGroupName, logGroup := range config.logGroups {
		if selectedLogGroup == "" || logGroupName == selectedLogGroup {
			startLogGroupParserTest(logGroup, perfInfo)
		}
	}
}
