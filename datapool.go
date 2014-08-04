package logmetrics

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/mathpl/go-timemetrics"
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
	data               timemetrics.Metric
	filename           string
	last_push          time.Time
	last_crunched_push time.Time
}

type fileInfo struct {
	lastUpdate time.Time
	last_push  time.Time
}

type DataPool struct {
	name      string
	hostname  string
	data      map[string]*tsdPoint
	tsd_push  chan []string
	tail_data chan lineResult

	channel_number     int
	tsd_channel_number int

	stale_removal          bool
	out_of_order_time_warn bool
	log_stale_metrics      bool
	fail_operation_warn    bool
	send_duplicates        bool

	date_position int
	date_format   string

	interval         int
	expected_matches int
	key_prefix       string
	tags             map[string]int
	metrics          map[int][]KeyExtract

	histogram_size                  int
	histogram_alpha_decay           float64
	histogram_rescale_threshold_min int
	ewma_interval                   int
	stale_treshold_min              int

	total_keys     int
	total_stale    int
	last_time_file map[string]fileInfo

	Bye chan bool
}

func (dp *DataPool) getNbTags() int {
	return len(dp.tags)
}

func (dp *DataPool) getNbKeys() int {
	i := 0
	for _, metrics := range dp.metrics {
		i += len(metrics)
	}
	return i
}

func (dp *DataPool) extractTags(data []string) []string {
	tags := make([]string, len(dp.tags))

	i := 0

	//General tags
	for tagname, position := range dp.tags {
		tags[i] = fmt.Sprintf("%s=%s", tagname, data[position])
		i++
	}

	return tags
}

func (dp *DataPool) getKeys(data []string) ([]dataPoint, time.Time) {
	y := time.Now().Year()

	tags := dp.extractTags(data)

	nbKeys := dp.getNbKeys()
	dataPoints := make([]dataPoint, nbKeys)

	//Time
	t, err := time.Parse(dp.date_format, data[dp.date_position])
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
	values := make([]int64, dp.expected_matches+1)
	for position, keyTypes := range dp.metrics {
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
		for _, keyType := range dp.metrics[position] {
			//Key name
			key := fmt.Sprintf("%s.%s.%s %s %s", dp.key_prefix, keyType.key_suffix, "%s %d %s", strings.Join(tags, " "), keyType.tag)

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

			if val < 0 && dp.fail_operation_warn {
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

func (dp *DataPool) getStatsKey(timePush time.Time) []string {
	line := make([]string, 2)
	line[0] = fmt.Sprintf("logmetrics_collector.data_pool.key_tracked %d %d host=%s log_group=%s log_group_number=%d", timePush.Unix(), dp.total_keys, dp.hostname, dp.name, dp.tsd_channel_number)
	line[1] = fmt.Sprintf("logmetrics_collector.data_pool.key_staled %d %d host=%s log_group=%s log_group_number=%d", timePush.Unix(), dp.total_stale, dp.hostname, dp.name, dp.tsd_channel_number)

	return line
}

func (dp *DataPool) start() {
	log.Printf("Datapool[%s:%d] started. Pushing keys to TsdPusher[%d]", dp.name, dp.channel_number, dp.tsd_channel_number)

	//Start the handler
	go func() {
		var last_time_pushed *time.Time
		var lastTimeStatsPushed time.Time
		for {
			select {
			case line_result := <-dp.tail_data:
				data_points, point_time := dp.getKeys(line_result.matches)

				if currentFileInfo, ok := dp.last_time_file[line_result.filename]; ok {
					if currentFileInfo.lastUpdate.Before(point_time) {
						currentFileInfo.lastUpdate = point_time
					}
				} else {
					dp.last_time_file[line_result.filename] = fileInfo{lastUpdate: point_time}
				}

				//To start things off
				if last_time_pushed == nil {
					last_time_pushed = &point_time
				}

				for _, data_point := range data_points {
					//New metrics, add
					if _, ok := dp.data[data_point.name]; !ok {
						switch data_point.metric_type {
						case "histogram":
							s := timemetrics.NewExpDecaySample(point_time, dp.histogram_size, dp.histogram_alpha_decay, dp.histogram_rescale_threshold_min)
							dp.data[data_point.name] = &tsdPoint{data: timemetrics.NewHistogram(s, dp.stale_treshold_min),
								last_push: point_time, filename: line_result.filename}
						case "counter":
							dp.data[data_point.name] = &tsdPoint{data: timemetrics.NewCounter(point_time, dp.stale_treshold_min),
								last_push: point_time, filename: line_result.filename}
						case "meter":
							dp.data[data_point.name] = &tsdPoint{data: timemetrics.NewMeter(point_time, dp.ewma_interval, dp.stale_treshold_min),
								last_push: point_time, last_crunched_push: point_time, filename: line_result.filename}
						default:
							log.Fatalf("Unexpected metric type %s!", data_point.metric_type)
						}
					}

					//Make sure data is ordered or we risk sending duplicate data
					if dp.data[data_point.name].last_push.Unix() > point_time.Unix() && dp.out_of_order_time_warn {
						log.Printf("Non-ordered data detected in log file. Its key already had a update at %s in the future. Offending line: %s",
							dp.data[data_point.name].last_push, line_result.matches[0])
					}

					dp.data[data_point.name].data.Update(point_time, data_point.value)
					dp.data[data_point.name].filename = line_result.filename
				}

				//Support for log playback - Push when <interval> has pass in the logs, not real time
				run_push_keys := false
				if dp.stale_removal && point_time.Sub(*last_time_pushed) >= time.Duration(dp.interval)*time.Second {
					run_push_keys = true
				} else if !dp.stale_removal {
					// Check for each file individually
					for _, fileInfo := range dp.last_time_file {
						if point_time.Sub(fileInfo.last_push) >= time.Duration(dp.interval)*time.Second {
							run_push_keys = true
							break
						}
					}
				}

				if run_push_keys {
					var nb_stale int
					dp.total_keys, nb_stale = dp.pushKeys(point_time)
					dp.total_stale += nb_stale

					//Push stats as well?
					if point_time.Sub(lastTimeStatsPushed) > time.Duration(dp.interval)*time.Second {
						dp.tsd_push <- dp.getStatsKey(point_time)
						lastTimeStatsPushed = point_time
					}

					last_time_pushed = &point_time
				}
			}
		}
	}()
}

func (dp *DataPool) pushKeys(point_time time.Time) (int, int) {
	nbKeys := 0
	nbStale := 0
	for tsd_key, tsdPoint := range dp.data {
		pointData := tsdPoint.data
		currentFileInfo := dp.last_time_file[tsdPoint.filename]

		if dp.stale_removal && pointData.Stale(point_time) {
			if dp.log_stale_metrics {
				log.Printf("Deleting stale metric. Last update: %s Current time: %s Metric: %s", pointData.GetMaxTime(), point_time, tsd_key)
			}

			//Push the zeroed-out key one last time to stabilize aggregated data
			pointData.ZeroOut()
			delete(dp.data, tsd_key)
			nbStale += pointData.NbKeys()
		} else {
			nbKeys += pointData.NbKeys()
		}

		if dp.send_duplicates || pointData.PushKeysTime(tsdPoint.last_push) {
			tsdPoint.last_push = pointData.GetMaxTime()
			currentFileInfo.last_push = tsdPoint.last_push

			// When sending duplicate use the current time instead of the lawst updated time of the metric.
			keys := pointData.GetKeys(point_time, tsd_key, dp.send_duplicates)

			dp.tsd_push <- keys
		}
	}

	return nbKeys, nbStale
}

func StartDataPools(config *Config, tsd_pushers []chan []string) (dps []*DataPool) {
	//Start a queryHandler by log group
	nb_tsd_push := 0
	dps = make([]*DataPool, 0)
	for _, lg := range config.logGroups {
		for i := 0; i < lg.goroutines; i++ {
			dp := lg.CreateDataPool(i, tsd_pushers, nb_tsd_push)
			dp.start()
			dps = append(dps, dp)

			nb_tsd_push = (nb_tsd_push + 1) % config.GetPusherNumber()
		}
	}

	return dps
}
