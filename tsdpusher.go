package logmetrics

import (
	"fmt"
	//"io"
	"log"
	"net"
	//"os"
	"time"
)

type keyPushStats struct {
	key_pushed  int64
	byte_pushed int64
	last_report time.Time
}

func (f *keyPushStats) inc(data_written int) {
	f.key_pushed++
	f.byte_pushed += int64(data_written)
}

func (f *keyPushStats) getStats() string {
	key_sec := int(f.key_pushed / int64(time.Now().Sub(f.last_report)/time.Second))
	mbyte_sec := float64(f.byte_pushed) / 1024 / 1024 / float64(time.Now().Sub(f.last_report)/time.Second)

	f.key_pushed = 0
	f.byte_pushed = 0
	f.last_report = time.Now()

	return fmt.Sprintf("%d key/s. %.3f Mb/s.",
		key_sec, mbyte_sec)
}

func (f *keyPushStats) isTimeForStats(interval int) bool {
	return (time.Now().Sub(f.last_report) > time.Duration(interval)*time.Second)
}

func StartTsdPushers(config *Config, tsd_pushers []chan []string, doNotSend bool) {
	if config.pushPort == 0 {
		return
	}

	//Get hostname
	//hostname, err := os.Hostname()
	//if err != nil {
	//	log.Fatalf("Unable to get hostname: ", err)
	//}

	//Open a connection to local push instance
	target := fmt.Sprintf("%s:%d", config.pushHost, config.pushPort)
	conn, err := net.Dial(config.pushProto, target)
	if err != nil && !doNotSend {
		log.Fatalf("Unable to connect to push on %s: %s", target, err)
	}

	for i, _ := range tsd_pushers {
		channel_number := i

		log.Printf("TsdPusher[%d] started. Pushing keys to %s:%d over %s in %s format", channel_number, config.pushHost,
			config.pushPort, config.pushProto, config.pushType)

		tsd_push := tsd_pushers[channel_number]
		go func() {
			defer conn.Close()

			key_push_stats := keyPushStats{last_report: time.Now()}

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

			for keys := range tsd_push {
				for _, key := range keys {
					var sentOk = false
					for !sentOk {
						var line string
						if config.pushType == "tsd" {
							line = ("put " + key + "\n")
						} else {
							line = key + "\n"
						}

						byte_line := []byte(line)

						var err error
						//var data_witten int
						if conn != nil {
							if !doNotSend {
								/*data_witten*/ _, err = conn.Write(byte_line)
							} else {
								fmt.Print(line)
							}
						}

						if err != nil {
							log.Printf("Error sending data to push: %s", err)
							time.Sleep(time.Duration(config.pushWait) * time.Second)
							conn.Close()
							conn = nil
						} else {
							key_push_stats.inc(len(line))
							sentOk = true
						}

						//Reconnect
						if conn == nil {
							if conn, err = net.Dial(config.pushProto, target); err != nil {
								log.Printf("Unable to reconnect: %s", err)
								time.Sleep(time.Duration(config.pushWait) * time.Second)
							} else {
								log.Printf("Reconnected")
							}
						}

						//Stats on key pushed
						//log.Printf("%d > %d", time.Now().Sub(key_push_stats.last_key_push_report), time.Duration(config.pollInterval)*time.Second)
						if key_push_stats.isTimeForStats(config.pollInterval) {
							log.Printf("TsdPusher[%d] %s", channel_number, key_push_stats.getStats())
						}
					}
				}
			}
		}()
	}
}
