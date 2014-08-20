package logmetrics

import (
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/mathpl/tail"
)

type tailer struct {
	ts             tailStats
	filename       string
	channel_number int
	tsd_pusher     chan []string

	lg *logGroup

	Bye chan bool
}

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

func (t *tailer) tailFile() {
	t.ts = tailStats{last_report: time.Now(), hostname: getHostname(),
		filename: t.filename, log_group: t.lg.name, interval: t.lg.interval}

	maxMatches := t.lg.expected_matches + 1

	var filename_matches []string
	if t.lg.filename_match_re != nil {
		m := t.lg.filename_match_re.MatcherString(t.filename, 0)
		filename_matches = m.ExtractString()[1:]
	}

	//os.Seek end of file descriptor
	seekParam := 2
	if t.lg.parse_from_start {
		seekParam = 0
	}

	loc := tail.SeekInfo{0, seekParam}

	tail, err := tail.TailFile(t.filename, tail.Config{Location: &loc, Follow: true, ReOpen: true, Poll: t.lg.poll_file})
	if err != nil {
		log.Fatalf("Unable to tail %s: %s", t.filename, err)
		return
	}
	log.Printf("Tailing %s data to datapool[%s:%d]", t.filename, t.lg.name, t.channel_number)

	//FIXME: Bug in ActiveTail can get partial lines
	for {
		select {
		case line := <-tail.Lines:
			if line.Err != nil {
				log.Printf("Tail on %s was lost: %s", t.filename, err)
				return
			}

			//Test out all the regexp, pick the first one that matches
			match_one := false
			for _, re := range t.lg.re {
				m := re.MatcherString(line.Text, 0)
				matches := m.ExtractString()
				if len(matches) == maxMatches {
					match_one = true
					if filename_matches != nil {
						matches = append(matches, filename_matches[:]...)
					}

					results := lineResult{t.filename, matches}
					t.lg.tail_data[t.channel_number] <- results
					t.ts.incLineMatch()
					break
				}
			}

			t.ts.incLine(line.Text)

			if t.lg.fail_regex_warn && !match_one {
				log.Printf("Regexp match failed on %s, expected %d matches: %s", t.filename, maxMatches, line.Text)
			}

			if (t.ts.line_read%100) == 0 && t.ts.isTimeForStats() {
				t.tsd_pusher <- t.ts.getTailStatsKey()
			}
		case <-t.Bye:
			log.Printf("Tailer for %s stopped.", t.filename)
			return
		}
	}
}

type filenamePoller struct {
	lg            *logGroup
	poll_interval int
	tsd_pushers   []chan []string
	push_number   int

	Bye chan bool
}

func (fp *filenamePoller) startFilenamePoller() {
	log.Printf("Filename poller for %s started", fp.lg.name)
	log.Printf("Using the following regexp for log group %s: %s", fp.lg.name, fp.lg.strRegexp)

	rescanFiles := make(chan bool, 1)
	go func() {
		rescanFiles <- true
		for {
			time.Sleep(time.Duration(fp.poll_interval) * time.Second)
			rescanFiles <- true
		}
	}()

	currentFiles := make(map[string]bool)
	channel_number := 0
	pusher_channel_number := 0

	allTailers := make([]*tailer, 0)
	for {
		select {
		case <-rescanFiles:
			newFiles := make(map[string]bool)
			for _, glob := range fp.lg.globFiles {
				files, err := filepath.Glob(glob)
				if err != nil {
					log.Fatalf("Unable to find files for log group %s: %s", fp.lg.name, err)
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
				bye := make(chan bool)
				t := tailer{filename: file, channel_number: channel_number, lg: fp.lg, Bye: bye,
					tsd_pusher: fp.tsd_pushers[pusher_channel_number]}
				go t.tailFile()
				allTailers = append(allTailers, &t)

				channel_number = (channel_number + 1) % fp.lg.goroutines
				pusher_channel_number = (pusher_channel_number + 1) % fp.push_number

				currentFiles[file] = true
			}
		case <-fp.Bye:
			for _, t := range allTailers {
				go func() { t.Bye <- true }()
			}
			log.Printf("Filename poller for %s stopped", fp.lg.name)
			return
		}
	}
}

func StartTails(config *Config, tsd_pushers []chan []string) []*filenamePoller {
	filenamePollers := make([]*filenamePoller, 0)
	for _, logGroup := range config.logGroups {
		bye := make(chan bool)
		f := filenamePoller{lg: logGroup, poll_interval: config.pollInterval, tsd_pushers: tsd_pushers, push_number: config.GetPusherNumber(), Bye: bye}
		filenamePollers = append(filenamePollers, &f)
		go f.startFilenamePoller()
	}

	return filenamePollers
}
