package logmetrics

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mathpl/go-timemetrics"
)

type dataPoint struct {
	name        string
	value       int64
	never_stale bool
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
	never_stale        bool
}

type fileInfo struct {
	lastUpdate time.Time
	last_push  time.Time
}

type datapool struct {
	data          map[string]*tsdPoint
	duplicateSent map[string]time.Time
	tsd_push      chan []string
	tail_data     chan lineResult

	channel_number     int
	tsd_channel_number int

	tag_order []string

	lg *logGroup

	total_keys     int
	total_stale    int
	last_time_file map[string]fileInfo

	Bye chan bool
}

func (dp *datapool) compileTagOrder() {
	tag_order := make([]string, dp.lg.getNbTags())
	i := 0
	for tagname, _ := range dp.lg.tags {
		tag_order[i] = tagname
		i++
	}
	sort.Strings(tag_order)

	dp.tag_order = tag_order
}

func (dp *datapool) extractTags(data []string) []string {
	//General tags
	tags := make([]string, dp.lg.getNbTags())
	for cnt, tagname := range dp.tag_order {

		tag_value := ""
		pos_or_value := dp.lg.tags[tagname]

		switch pos_or_string := pos_or_value.(type) {
		case int:
			tag_value = data[pos_or_string]
		case string:
			tag_value = pos_or_string
		}

		tags[cnt] = fmt.Sprintf("%s=%s", tagname, tag_value)
	}

	return tags
}

func build_replace_map(data []string) map[string]string {
	r := make(map[string]string)

	for pos, s := range data {
		r[fmt.Sprintf("%d", pos)] = s
	}

	return r
}

func (dp *datapool) applyTransforms(match_groups []string) []string {
	transformed_matches := make([]string, len(match_groups))

	for pos, data := range match_groups {
		if transform, ok := dp.lg.transform[pos]; ok {
			transformed_matches[pos] = transform.apply(data)
		} else {
			transformed_matches[pos] = data
		}
	}

	return transformed_matches
}

func (dp *datapool) getKeys(data []string) ([]dataPoint, time.Time) {
	y := time.Now().Year()

	tags := dp.extractTags(data)

	nbKeys := dp.lg.getNbKeys()
	dataPoints := make([]dataPoint, nbKeys)

	//Time
	t, err := time.Parse(dp.lg.date_format, data[dp.lg.date_position])
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
	values := make([]int64, dp.lg.expected_matches+1)
	for position, keyTypes := range dp.lg.metrics {
		for _, keyType := range keyTypes {
			if position == 0 {
				values[position] = 1
			} else {
				var val int64
				var err error
				if keyType.format == "float" {
					var val_float float64
					if val_float, err = strconv.ParseFloat(data[position], 64); err == nil {
						if keyType.multiply > 1 {
							val = int64(val_float * float64(keyType.multiply))
						}
						if keyType.divide > 1 {
							val = int64(val_float / float64(keyType.divide))
						}
					}
				} else {
					if val, err = strconv.ParseInt(data[position], 10, 64); err == nil {
						if keyType.multiply > 1 {
							val = val * int64(keyType.multiply)
						}
						if keyType.divide > 1 {
							val = val / int64(keyType.divide)
						}
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

	// Second pass applies operation and create datapoints
	var i = 0
	for position, val := range values {
		//Is the value a metric?
		for _, keyType := range dp.lg.metrics[position] {
			//Key name
			key := fmt.Sprintf("%s.%s.%s %s %s", dp.lg.key_prefix, keyType.key_suffix, "%s %d %s", strings.Join(tags, " "), keyType.tag)

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

			if val < 0 && dp.lg.fail_operation_warn {
				log.Printf("Values cannot be negative after applying operation. Offending line: %s", data[0])
				var nt time.Time
				return nil, nt
			}

			dataPoints[i] = dataPoint{name: key, value: val, metric_type: keyType.metric_type, never_stale: keyType.never_stale}
			i++
		}
	}

	return dataPoints, t
}

func (dp *datapool) getStatsKey(timePush time.Time) []string {
	line := make([]string, 2)
	line[0] = fmt.Sprintf("logmetrics_collector.data_pool.key_tracked %d %d host=%s log_group=%s log_group_number=%d", timePush.Unix(), dp.total_keys, dp.lg.hostname, dp.lg.name, dp.tsd_channel_number)
	line[1] = fmt.Sprintf("logmetrics_collector.data_pool.key_staled %d %d host=%s log_group=%s log_group_number=%d", timePush.Unix(), dp.total_stale, dp.lg.hostname, dp.lg.name, dp.tsd_channel_number)

	return line
}

func (dp *datapool) start() {
	log.Printf("Datapool[%s:%d] started. Pushing keys to TsdPusher[%d]", dp.lg.name, dp.channel_number, dp.tsd_channel_number)

	var last_time_pushed *time.Time
	var lastTimeStatsPushed time.Time
	for {
		select {
		case line_result := <-dp.tail_data:

			transformed_matches := dp.applyTransforms(line_result.matches)

			data_points, point_time := dp.getKeys(transformed_matches)

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
						s := timemetrics.NewExpDecaySample(point_time, dp.lg.histogram_size, dp.lg.histogram_alpha_decay, dp.lg.histogram_rescale_threshold_min)
						dp.data[data_point.name] = &tsdPoint{data: timemetrics.NewHistogram(s, dp.lg.stale_treshold_min),
							filename: line_result.filename}
					case "counter":
						dp.data[data_point.name] = &tsdPoint{data: timemetrics.NewCounter(point_time, dp.lg.stale_treshold_min),
							filename: line_result.filename}
					case "meter":
						dp.data[data_point.name] = &tsdPoint{data: timemetrics.NewMeter(point_time, dp.lg.ewma_interval, dp.lg.stale_treshold_min),
							filename: line_result.filename}
					default:
						log.Fatalf("Unexpected metric type %s!", data_point.metric_type)
					}
				}

				//Make sure data is ordered or we risk sending duplicate data
				if dp.data[data_point.name].last_push.After(point_time) && dp.lg.out_of_order_time_warn {
					log.Printf("Non-ordered data detected in log file. Its key already had a update at %s in the future. Offending line: %s",
						dp.data[data_point.name].last_push, line_result.matches[0])
				}

				dp.data[data_point.name].data.Update(point_time, data_point.value)
				dp.data[data_point.name].filename = line_result.filename
				dp.data[data_point.name].never_stale = data_point.never_stale
			}

			//Support for log playback - Push when <interval> has pass in the logs, not real time
			run_push_keys := false
			if dp.lg.live_poll && point_time.Sub(*last_time_pushed) >= time.Duration(dp.lg.interval)*time.Second {
				run_push_keys = true
			} else if !dp.lg.stale_removal {
				// Check for each file individually
				for _, fileInfo := range dp.last_time_file {
					if point_time.Sub(fileInfo.last_push) >= time.Duration(dp.lg.interval)*time.Second {
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
				if point_time.Sub(lastTimeStatsPushed) > time.Duration(dp.lg.interval)*time.Second {
					dp.tsd_push <- dp.getStatsKey(point_time)
					lastTimeStatsPushed = point_time
				}

				last_time_pushed = &point_time
			}
		case <-dp.Bye:
			log.Printf("Datapool[%s:%d] stopped.", dp.lg.name, dp.channel_number)
			return
		}
	}
}

func (dp *datapool) pushKeys(point_time time.Time) (int, int) {
	nbKeys := 0
	nbStale := 0
	for tsd_key, tsdPoint := range dp.data {
		pointData := tsdPoint.data
		currentFileInfo := dp.last_time_file[tsdPoint.filename]

		if dp.lg.stale_removal && pointData.Stale(point_time) && !tsdPoint.never_stale {
			if dp.lg.log_stale_metrics {
				log.Printf("Deleting stale metric. Last update: %s Current time: %s Metric: %s", pointData.GetMaxTime(), point_time, tsd_key)
			}

			//Push the zeroed-out key one last time to stabilize aggregated data
			pointData.ZeroOut()
			delete(dp.data, tsd_key)
			nbStale += pointData.NbKeys()
		} else {
			nbKeys += pointData.NbKeys()
		}

		// pointData.lastUpdate.After(tsdPoint.last_push)
		updateToSend := pointData.PushKeysTime(tsdPoint.last_push)

		var keys []string
		if updateToSend {
			tsdPoint.last_push = pointData.GetMaxTime()
			currentFileInfo.last_push = tsdPoint.last_push

			// always take the log file timestamp
			keys = pointData.GetKeys(point_time, tsd_key, false)
		} else if !updateToSend && dp.lg.send_duplicates {
			var dup_time time.Time
			if _,ok := dp.duplicateSent[tsd_key]; ok {
				dup_time = dp.duplicateSent[tsd_key].Add((time.Second * time.Duration(dp.lg.interval)))
			} else {
				dup_time = pointData.GetMaxTime().Add((time.Second * time.Duration(dp.lg.interval)))
			}

			dp.duplicateSent[tsd_key] = dup_time
			keys = pointData.GetKeys(dup_time, tsd_key, true)
		}

		dp.tsd_push <- keys

		if currentFileInfo.last_push.After(dp.last_time_file[tsdPoint.filename].last_push) {
			dp.last_time_file[tsdPoint.filename] = currentFileInfo
		}
	}

	return nbKeys, nbStale
}

func StartDataPools(config *Config, tsd_pushers []chan []string) (dps []*datapool) {
	//Start a queryHandler by log group
	nb_tsd_push := 0
	dps = make([]*datapool, 0)
	for _, lg := range config.logGroups {
		for i := 0; i < lg.goroutines; i++ {
			dp := lg.CreateDataPool(i, tsd_pushers, nb_tsd_push)
			go dp.start()
			dps = append(dps, dp)

			nb_tsd_push = (nb_tsd_push + 1) % config.GetPusherNumber()
		}
	}

	return dps
}
