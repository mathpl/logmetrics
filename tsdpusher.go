package logmetrics

import (
	"fmt"
	"log"
	"net"
	"time"
)

type pusher struct {
	cfg            *Config
	tsd_push       chan []string
	do_not_send    bool
	channel_number int
	hostname       string
	key_push_stats keyPushStats

	Bye chan bool
}

type keyPushStats struct {
	key_pushed    int64
	byte_pushed   int64
	last_report   time.Time
	hostname      string
	interval      int
	pusher_number int
}

func (f *keyPushStats) inc(data_written int) {
	f.key_pushed++
	f.byte_pushed += int64(data_written)
}

func (f *keyPushStats) getLine() []string {
	t := time.Now()

	f.last_report = t

	line := make([]string, 2)
	line[0] = fmt.Sprintf("logmetrics_collector.pusher.key_sent %d %d host=%s pusher_number=%d", t.Unix(), f.key_pushed, f.hostname, f.pusher_number)
	line[1] = fmt.Sprintf("logmetrics_collector.pusher.byte_sent %d %d host=%s pusher_number=%d", t.Unix(), f.byte_pushed, f.hostname, f.pusher_number)

	return line
}

func (f *keyPushStats) isTimeForStats() bool {
	return time.Now().Sub(f.last_report) > time.Duration(f.interval)*time.Second
}

func writeLine(config *Config, do_not_send bool, conn net.Conn, line string) (int, net.Conn) {
	if config.pushType == "tsd" {
		line = ("put " + line + "\n")
	} else {
		line = line
	}

	byte_line := []byte(line)
	byte_written := len(byte_line)

	var err error
	if do_not_send {
		fmt.Print(line + "\n")
	} else {
		for {
			//Reconnect if needed
			if conn == nil {
				target := config.GetTsdTarget()
				log.Printf("Reconnecting to %s", target)

				if conn, err = net.Dial(config.pushProto, target); err != nil {
					log.Printf("Unable to reconnect: %s", err)
					time.Sleep(time.Duration(config.pushWait) * time.Second)
				}
			}

			if conn != nil {
				_, err = conn.Write(byte_line)

				if err != nil {
					log.Printf("Error writting data: %s", err)
					conn.Close()
					conn = nil
					time.Sleep(time.Duration(config.pushWait) * time.Second)
				} else {
					break
				}
			}

		}
	}

	return byte_written, conn
}

func (p *pusher) start() {
	log.Printf("TsdPusher[%d] started. Pushing keys to %s:%d over %s in %s format", p.channel_number, p.cfg.pushHost,
		p.cfg.pushPort, p.cfg.pushProto, p.cfg.pushType)

	p.key_push_stats = keyPushStats{last_report: time.Now(), hostname: p.hostname, interval: p.cfg.stats_interval, pusher_number: p.channel_number}

	var conn net.Conn
	for {
		select {
		case keys := <-p.tsd_push:
			for _, line := range keys {
				var bytes_written int
				bytes_written, conn = writeLine(p.cfg, p.do_not_send, conn, line)

				p.key_push_stats.inc(bytes_written)

				//Stats on key pushed, limit checks with modulo (now() is a syscall)
				if (p.key_push_stats.key_pushed%100) == 0 && p.key_push_stats.isTimeForStats() {
					for _, local_line := range p.key_push_stats.getLine() {
						bytes_written, conn = writeLine(p.cfg, p.do_not_send, conn, local_line)
						p.key_push_stats.inc(bytes_written)
					}
				}
			}
		case <-p.Bye:
			log.Printf("TsdPusher[%d] stopped.", p.channel_number)
			return
		}
	}
}

func StartTsdPushers(config *Config, tsd_pushers []chan []string, do_not_send bool) []*pusher {
	if config.pushPort == 0 {
		return nil
	}

	hostname := getHostname()

	allPushers := make([]*pusher, 0)
	for i, _ := range tsd_pushers {
		channel_number := i

		tsd_push := tsd_pushers[channel_number]
		bye := make(chan bool)
		p := pusher{cfg: config, tsd_push: tsd_push, hostname: hostname, do_not_send: do_not_send, channel_number: channel_number, Bye: bye}
		go p.start()
		allPushers = append(allPushers, &p)
	}

	return allPushers
}
