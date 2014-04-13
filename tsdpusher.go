package logmetrics

import (
	"fmt"
	"log"
	"net"
	"time"
)

type keyPushStats struct {
	key_pushed     int64
	byte_pushed    int64
	last_report    time.Time
	hostname       string
	interval       int
	channel_number int
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

func writeLine(config *Config, doNotSend bool, conn net.Conn, line string) (int, net.Conn) {
	if config.pushType == "tsd" {
		line = ("put " + line + "\n")
	} else {
		line = line + "\n"
	}

	byte_line := []byte(line)
	byte_written := len(byte_line)

	var err error
	if doNotSend {
		fmt.Print(line)
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

func StartTsdPushers(config *Config, tsd_pushers []chan []string, doNotSend bool) {
	if config.pushPort == 0 {
		return
	}

	hostname := getHostname()

	for i, _ := range tsd_pushers {
		channel_number := i

		log.Printf("TsdPusher[%d] started. Pushing keys to %s:%d over %s in %s format", channel_number, config.pushHost,
			config.pushPort, config.pushProto, config.pushType)

		tsd_push := tsd_pushers[channel_number]
		go func() {
			key_push_stats := keyPushStats{last_report: time.Now(), hostname: hostname, interval: config.stats_wait, channel_number: channel_number}

			//Check if TSD has something to say
			//if config.pushType == "tsd" {
			//	go func() {
			//		response_buffer := make([]byte, 1024)
			//		for {
			//			if conn != nil {
			//				if size, read_err := conn.Read(response_buffer); read_err != nil && read_err != io.EOF {
			//					log.Printf("Unable to read response: %s %+V", read_err, read_err)
			//				} else if size > 0 {
			//					log.Print(string(response_buffer))
			//				}
			//			}
			//		}
			//	}()
			//}

			var conn net.Conn
			for keys := range tsd_push {
				for _, line := range keys {
					var bytes_written int
					bytes_written, conn = writeLine(config, doNotSend, conn, line)

					key_push_stats.inc(bytes_written)

					//Stats on key pushed, limit checks with modulo (now() is a syscall)
					if (key_push_stats.key_pushed%10000) == 0 && key_push_stats.isTimeForStats() {
						for _, local_line := range key_push_stats.getLine() {
							bytes_written, conn = writeLine(config, doNotSend, conn, local_line)
							key_push_stats.inc(bytes_written)
						}
					}
				}
			}

		}()
	}
}
