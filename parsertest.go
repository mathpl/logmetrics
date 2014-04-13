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
	line_read    int64
	line_matched int64
	byte_pushed  int64
	last_report  time.Time
}

func (f *readStats) inc(matched bool, data_read int) {
	f.line_read++
	if matched {
		f.line_matched++
	}
	f.byte_pushed += int64(data_read)
}

func (f *readStats) getStats() string {
	line_sec := int(f.line_read / int64(time.Now().Sub(f.last_report)/time.Second))
	match_sec := int(f.line_matched / int64(time.Now().Sub(f.last_report)/time.Second))
	mbyte_sec := float64(f.byte_pushed) / 1024 / 1024 / float64(time.Now().Sub(f.last_report)/time.Second)

	f.line_read = 0
	f.line_matched = 0
	f.byte_pushed = 0
	f.last_report = time.Now()

	return fmt.Sprintf("%d line/s  %d match/s  %.3f Mb/s.",
		line_sec, match_sec, mbyte_sec)
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
			matches := m.Extract()
			//matches := buildMatches(line, m)
			if len(matches) == maxMatches {

				match_one = true
			}
		}

		read_stats.inc(match_one, len(line))

		if lg.fail_regex_warn && !match_one {
			log.Printf("Regexp match failed on %s, expected %d matches: %s", filename, maxMatches, line.Text)
		}

		if read_stats.isTimeForStats(1) {
			log.Print(read_stats.getStats())
		}
	}

	log.Printf("Finished parsing %s.", filename)
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
