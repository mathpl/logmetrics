package logmetrics

import (
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/mathpl/tail"
)

type tailStats struct {
	line_read   int64
	byte_read   int64
	line_match  int64
	last_report time.Time
	hostname    string
	filename    string
	log_group   string
	interval    int
}

type lineResult struct {
	filename string
	matches  []string
}

func (ts *tailStats) isTimeForStats() bool {
	return time.Now().Sub(ts.last_report) > time.Duration(ts.interval)*time.Second
}

func (ts *tailStats) incLineMatch() {
	ts.line_match++
}

func (ts *tailStats) incLine(line string) {
	ts.line_read++
	ts.byte_read += int64(len(line))
}

func (ts *tailStats) getTailStatsKey() []string {
	t := time.Now()

	ts.last_report = t

	line := make([]string, 3)
	line[0] = fmt.Sprintf("logmetrics_collector.tail.line_read %d %d host=%s log_group=%s filename=%s", t.Unix(), ts.line_read, ts.hostname, ts.log_group, ts.filename)
	line[1] = fmt.Sprintf("logmetrics_collector.tail.byte_read %d %d host=%s log_group=%s filename=%s", t.Unix(), ts.byte_read, ts.hostname, ts.log_group, ts.filename)
	line[2] = fmt.Sprintf("logmetrics_collector.tail.line_matched %d %d host=%s log_group=%s filename=%s", t.Unix(), ts.line_match, ts.hostname, ts.log_group, ts.filename)

	return line

}

func tailFile(channel_number int, filename string, lg *LogGroup, tsd_pusher chan []string) {
	tail_stats := tailStats{last_report: time.Now(), hostname: getHostname(),
		filename: filename, log_group: lg.name, interval: lg.interval}

	maxMatches := lg.expected_matches + 1

	var filename_matches []string
	if lg.filename_match_re != nil {
		m := lg.filename_match_re.MatcherString(filename, 0)
		filename_matches = m.ExtractString()[1:]
	}

	//os.Seek end of file descriptor
	seekParam := 2
	if lg.parse_from_start {
		seekParam = 0
	}

	loc := tail.SeekInfo{0, seekParam}

	tail, err := tail.TailFile(filename, tail.Config{Location: &loc, Follow: true, ReOpen: true, Poll: lg.poll_file})
	if err != nil {
		log.Fatalf("Unable to tail %s: %s", filename, err)
		return
	}
	log.Printf("Tailing %s data to datapool[%s:%d]", filename, lg.name, channel_number)

	//FIXME: Bug in ActiveTail can get partial lines
	for line := range tail.Lines {
		if line.Err != nil {
			log.Printf("Tail on %s was lost: %s", filename, err)
			return
		}

		//Test out all the regexp, pick the first one that matches
		match_one := false
		for _, re := range lg.re {
			m := re.MatcherString(line.Text, 0)
			matches := m.ExtractString()
			if len(matches) == maxMatches {
				match_one = true
				if filename_matches != nil {
					matches = append(matches, filename_matches[:]...)
				}

				results := lineResult{filename, matches}
				lg.tail_data[channel_number] <- results
				tail_stats.incLineMatch()
				break
			}
		}

		tail_stats.incLine(line.Text)

		if lg.fail_regex_warn && !match_one {
			log.Printf("Regexp match failed on %s, expected %d matches: %s", filename, maxMatches, line.Text)
		}

		if (tail_stats.line_read%100) == 0 && tail_stats.isTimeForStats() {
			tsd_pusher <- tail_stats.getTailStatsKey()
		}
	}

	log.Printf("Finished tailling %s.", filename)
}

func startLogGroup(logGroup *LogGroup, pollInterval int, tsd_pushers []chan []string, push_number int) {
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
	pusher_channel_number := 0
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
				go tailFile(channel_number, file, logGroup, tsd_pushers[pusher_channel_number])
				channel_number = (channel_number + 1) % logGroup.goroutines
				pusher_channel_number = (pusher_channel_number + 1) % push_number

				currentFiles[file] = true
			}
		}
	}
}

func StartTails(config *Config, tsd_pushers []chan []string) {
	for _, logGroup := range config.logGroups {
		go startLogGroup(logGroup, config.pollInterval, tsd_pushers, config.GetPusherNumber())
	}
}
