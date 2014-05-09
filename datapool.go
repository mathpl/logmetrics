package logmetrics

import (
	"fmt"
	"github.com/mathpl/go-timemetrics"
	"log"
	"strconv"
	"strings"
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
	filename         string
	lastPush         time.Time
	lastCrunchedPush time.Time
}

type fileInfo struct {
	lastUpdate time.Time
	lastPush   time.Time
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
	t, err := time.Parse(lg.date_format, data[lg.date_position])
	if err != nil {
		log.Print(err)
		var nt time.Time
		return nil, nt
	}

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

func (lg *LogGroup) getStatsKey(hostname string, nbKeys int, totalStale int, timePush time.Time, tsd_channel_number int) []string {
	line := make([]string, 2)
	line[0] = fmt.Sprintf("logmetrics_collector.data_pool.key_tracked %d %d host=%s log_group=%s log_group_number=%d", timePush.Unix(), nbKeys, hostname, lg.name, tsd_channel_number)
	line[1] = fmt.Sprintf("logmetrics_collector.data_pool.key_staled %d %d host=%s log_group=%s log_group_number=%d", timePush.Unix(), totalStale, hostname, lg.name, tsd_channel_number)

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

		totalStale := 0
		var lastTimePushed *time.Time
		var lastTimeStatsPushed time.Time
		lastTimeByFile := make(map[string]fileInfo)
		for {
			select {
			case lineResult := <-lg.tail_data[channel_number]:
				data_points, point_time := lg.getKeys(lineResult.matches)

				if currentFileInfo, ok := lastTimeByFile[lineResult.filename]; ok {
					if currentFileInfo.lastUpdate.Before(point_time) {
						currentFileInfo.lastUpdate = point_time
					}
				} else {
					lastTimeByFile[lineResult.filename] = fileInfo{lastUpdate: point_time}
				}

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
							dataPool[data_point.name] = &tsdPoint{data: timemetrics.NewHistogram(s, lg.stale_treshold_min),
								lastPush: point_time, filename: lineResult.filename}
						case "counter":
							dataPool[data_point.name] = &tsdPoint{data: timemetrics.NewCounter(point_time, lg.stale_treshold_min),
								lastPush: point_time, filename: lineResult.filename}
						case "meter":
							dataPool[data_point.name] = &tsdPoint{data: timemetrics.NewMeter(point_time, lg.ewma_interval, lg.stale_treshold_min),
								lastPush: point_time, lastCrunchedPush: point_time, filename: lineResult.filename}
						default:
							log.Fatalf("Unexpected metric type %s!", data_point.metric_type)
						}
					}

					//Make sure data is ordered or we risk sending duplicate data
					if dataPool[data_point.name].lastPush.Unix() > point_time.Unix() && lg.out_of_order_time_warn {
						log.Printf("Non-ordered data detected in log file. Its key already had a update at %s in the future. Offending line: %s",
							dataPool[data_point.name].lastPush, lineResult.matches[0])
					}

					dataPool[data_point.name].data.Update(point_time, data_point.value)
					dataPool[data_point.name].filename = lineResult.filename
				}

				//Support for log playback - Push when <interval> has pass in the logs, not real time
				run_push_keys := false
				if lg.stale_removal && point_time.Sub(*lastTimePushed) >= time.Duration(lg.interval)*time.Second {
					run_push_keys = true
				} else if !lg.stale_removal {
					// Check for each file individually
					for _, fileInfo := range lastTimeByFile {
						if point_time.Sub(fileInfo.lastPush) >= time.Duration(lg.interval)*time.Second {
							run_push_keys = true
							break
						}
					}
				}

				if run_push_keys {
					nbKeys, nbStale := pushKeys(point_time, tsd_push, &dataPool, &lastTimeByFile, lg.stale_removal, lg.send_duplicates)
					totalStale += nbStale

					//Push stats as well?
					if point_time.Sub(lastTimeStatsPushed) > time.Duration(lg.interval)*time.Second {
						tsd_push <- lg.getStatsKey(hostname, nbKeys, totalStale, point_time, channel_number)
						lastTimeStatsPushed = point_time
					}

					lastTimePushed = &point_time
				}
			}
		}
	}()
}

func pushKeys(point_time time.Time, tsd_push chan []string, dataPool *map[string]*tsdPoint, lastTimeByFile *map[string]fileInfo, stale_removal bool, send_duplicates bool) (int, int) {
	nbKeys := 0
	nbStale := 0
	for tsd_key, tsdPoint := range *dataPool {
		data := tsdPoint.data
		currentFileInfo := (*lastTimeByFile)[tsdPoint.filename]

		if stale_removal && data.Stale(currentFileInfo.lastUpdate) {
			//Push the zeroed-out key one last time to stabilize aggregated data
			data.ZeroOut()
			delete(*dataPool, tsd_key)
			delete(*lastTimeByFile, tsdPoint.filename)
			nbStale += data.NbKeys()
		} else {
			nbKeys += data.NbKeys()
		}

		if send_duplicates || data.PushKeysTime(tsdPoint.lastPush) {
			tsdPoint.lastPush = data.GetMaxTime()
			currentFileInfo.lastPush = tsdPoint.lastPush

			// When sending duplicate use the current time instead of the lawst updated time of the metric.
			keys := data.GetKeys(point_time, tsd_key, send_duplicates)

			tsd_push <- keys
		}
	}

	return nbKeys, nbStale
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
