package logmetrics

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"syseng/go-timemetrics"
	"time"
)

type dataPoint struct {
	name        string
	value       int64
	metric_type string
}

type dataPointTime struct {
	name string
	time int64
}

type tsdPoint struct {
	data             timemetrics.Metric
	lastPush         time.Time
	lastCrunchedPush time.Time
}

func (lg *LogGroup) extractTags(data []string) []string {
	tags := make([]string, lg.getNbTags())

	i := 0

	//General tags
	for tagname, position := range lg.tags {
		tags[i] = fmt.Sprintf("%s=%s", tagname, data[position])
		i++
	}

	return tags
}

func (lg *LogGroup) getKeys(data []string) ([]dataPoint, time.Time) {
	y := time.Now().Year()

	tags := lg.extractTags(data)

	nbKeys := lg.getNbKeys()
	dataPoints := make([]dataPoint, nbKeys)

	//Time
	var t time.Time
	if data[lg.date_position] == lg.last_date_str {
		t = lg.last_date
	} else {
		var err error
		t, err = time.Parse(lg.date_format, data[lg.date_position])
		if err != nil {
			log.Print(err)
			var nt time.Time
			return nil, nt
		}
	}

	//Keep time around to only parse new dates
	lg.last_date_str = data[lg.date_position]
	lg.last_date = t

	//Patch in year if missing - rfc3164
	if t.Year() == 0 {
		t = time.Date(y, t.Month(), t.Day(), t.Hour(), t.Minute(),
			t.Second(), t.Nanosecond(), t.Location())
	}

	//Make a first pass extracting the data, applying float->int conversion on multiplier
	values := make([]int64, lg.expected_matches+1)
	for position, keyTypes := range lg.metrics {
		for _, keyType := range keyTypes {
			if position == 0 {
				values[position] = 1
			} else {
				var val int64
				var err error
				if keyType.format == "float" {
					var val_float float64
					if val_float, err = strconv.ParseFloat(data[position], 64); err == nil {
						val = int64(val_float * float64(keyType.multiply))
					}
				} else {
					if val, err = strconv.ParseInt(data[position], 10, 64); err == nil {
						val = val * int64(keyType.multiply)
					}
				}

				if err != nil {
					log.Printf("Unable to extract data from value match, %s: %s", err, data[position])
					var nt time.Time
					return nil, nt
				} else {
					values[position] = val
				}
			}
		}
	}

	//Second pass applies operation and create datapoints
	var i = 0
	for position, val := range values {
		//Is the value a metric?
		for _, keyType := range lg.metrics[position] {
			//Key name
			key := fmt.Sprintf("%s.%s.%s %s %s", lg.key_prefix, keyType.key_suffix, "%s %d %s", strings.Join(tags, " "), keyType.tag)

			//Do we need to do any operation on this val?
			for op, opvalues := range keyType.operations {
				for _, op_position := range opvalues {
					//log.Printf("%s %d on pos %d, current val: %d", op, op_position, position, val)
					if op_position != 0 {
						switch op {
						case "add":
							val += values[op_position]

						case "sub":
							val -= values[op_position]
						}
					}
				}
			}

			if val < 0 && lg.fail_operation_warn {
				log.Printf("Values cannot be negative after applying operation. Offending line: %s", data[0])
				var nt time.Time
				return nil, nt
			}

			dataPoints[i] = dataPoint{name: key, value: val, metric_type: keyType.metric_type}
			i++
		}
	}

	return dataPoints, t
}

func (lg *LogGroup) getStatsKey(hostname string, v int, timePush time.Time, tsd_channel_number int) []string {
	line := make([]string, 1)
	line[0] = fmt.Sprintf("logmetrics_collector.data_pool.key_tracked %d %d host=%s log_group=%s log_group_number=%d", timePush.Unix(), v, hostname, lg.name, tsd_channel_number)

	return line

}

func (lg *LogGroup) dataPoolHandler(channel_number int, tsd_pushers []chan []string, tsd_channel_number int) {
	dataPool := make(map[string]*tsdPoint)
	tsd_push := tsd_pushers[tsd_channel_number]

	hostname := getHostname()

	log.Printf("Datapool[%s:%d] started. Pushing keys to TsdPusher[%d]", lg.name, channel_number, tsd_channel_number)

	//Start the handler
	go func() {

		//Failsafe if anything goes really wrong
		//defer func() {
		//	if r := recover(); r != nil {
		//		log.Printf("Recovered error in %s: %s", lg.name, r)
		//	}
		//}()

		var lastTimePushed *time.Time
		var lastTimeStatsPushed time.Time
		lastNbKeys := 0
		for {
			select {
			case data := <-lg.tail_data[channel_number]:
				data_points, point_time := lg.getKeys(data)

				//To start things off
				if lastTimePushed == nil {
					lastTimePushed = &point_time
				}

				for _, data_point := range data_points {
					//New metrics, add
					if _, ok := dataPool[data_point.name]; !ok {
						switch data_point.metric_type {
						case "histogram":
							s := timemetrics.NewExpDecaySample(point_time, lg.histogram_size, lg.histogram_alpha_decay, lg.histogram_rescale_threshold_min)
							dataPool[data_point.name] = &tsdPoint{data: timemetrics.NewHistogram(s),
								lastPush: point_time}
						case "counter":
							dataPool[data_point.name] = &tsdPoint{data: timemetrics.NewCounter(point_time),
								lastPush: point_time}
						case "meter":
							dataPool[data_point.name] = &tsdPoint{data: timemetrics.NewMeter(point_time, lg.ewma_interval),
								lastPush: point_time, lastCrunchedPush: point_time}
						default:
							log.Fatalf("Unexpected metric type %s!", data_point.metric_type)
						}
					}

					//Make sure data is ordered or we risk sending duplicate data
					if dataPool[data_point.name].lastPush.Unix() > point_time.Unix() && lg.out_of_order_time_warn {
						log.Printf("Non-ordered data detected in log file. Its key already had a update at %s in the future. Offending line: %s",
							dataPool[data_point.name].lastPush, data[0])
					}

					dataPool[data_point.name].data.Update(point_time, data_point.value)
				}

				//Support for log playback - Push when <interval> has pass in the logs, not real time
				if point_time.Sub(*lastTimePushed) > (time.Duration(lg.interval) * time.Second) {
					nbKeys := pushStats(point_time, tsd_push, dataPool)

					t := time.Now()
					nt := t
					if lastNbKeys != nbKeys && nt.Sub(lastTimeStatsPushed) > time.Duration(lg.interval)*time.Second {
						tsd_push <- lg.getStatsKey(hostname, nbKeys, t, tsd_channel_number)
						lastTimeStatsPushed = time.Now()
					}

					lastNbKeys = nbKeys
					lastTimePushed = &point_time
				}
			}
		}
	}()
}

func pushStats(lastTimePushed time.Time, tsd_push chan []string, dataPool map[string]*tsdPoint) (nbKeys int) {
	for tsd_key, tsdPoint := range dataPool {
		if tsdPoint.data.GetMaxTime().Unix() > tsdPoint.lastPush.Unix() {
			tsdPoint.lastPush = tsdPoint.data.GetMaxTime()
			keys := tsdPoint.data.GetKeys(lastTimePushed, tsd_key)
			tsd_push <- keys
		} else if tsdPoint.data.Stale(lastTimePushed) {
			delete(dataPool, tsd_key)
		}

		nbKeys += tsdPoint.data.NbKeys()
	}

	return
}

func StartDataPools(config *Config, tsd_pushers []chan []string) {
	//Start a queryHandler by log group
	nb_tsd_push := 0
	for _, lg := range config.logGroups {
		for i := 0; i < lg.goroutines; i++ {
			lg.dataPoolHandler(i, tsd_pushers, nb_tsd_push)
			nb_tsd_push = (nb_tsd_push + 1) % config.GetPusherNumber()
		}
	}
}
