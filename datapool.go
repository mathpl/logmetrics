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
	data             interface{}
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

	nbKeys := len(lg.metrics)
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
	for position, keyType := range lg.metrics {
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

	//Second pass applies operation and create datapoints
	var i = 0
	for position, val := range values {
		//Is the value a metric?
		if keyType, ok := lg.metrics[position]; ok {
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

func getCounterKeys(name string, c timemetrics.Counter) []string {
	t := int(c.GetMaxTime().Unix())

	keys := make([]string, 1)
	keys[0] = fmt.Sprintf(name, "count", t, fmt.Sprintf("%d", c.Count()))

	return keys
}

func getMeterKeyCount(name string, m timemetrics.Meter) []string {
	t := int(m.GetMaxTime().Unix())

	keys := make([]string, 1)
	keys[0] = fmt.Sprintf(name, "count", t, fmt.Sprintf("%d", m.Count()))

	return keys
}

func getMeterKeyRates(name string, m timemetrics.Meter) []string {
	t := int(m.GetMaxEWMATime().Unix())

	keys := make([]string, 3)
	keys[0] = fmt.Sprintf(name, "rate._1min", t, fmt.Sprintf("%.4f", m.Rate1()))
	keys[1] = fmt.Sprintf(name, "rate._5min", t, fmt.Sprintf("%.4f", m.Rate5()))
	keys[2] = fmt.Sprintf(name, "rate._15min", t, fmt.Sprintf("%.4f", m.Rate15()))

	return keys
}

func getHistogramKeys(name string, h timemetrics.Histogram) []string {
	t := int(h.GetMaxTime().Unix())
	ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})

	keys := make([]string, 10)

	keys[0] = fmt.Sprintf(name, "min", t, fmt.Sprintf("%d", h.Min()))
	keys[1] = fmt.Sprintf(name, "max", t, fmt.Sprintf("%d", h.Max()))
	keys[2] = fmt.Sprintf(name, "mean", t, fmt.Sprintf("%.4f", h.Mean()))
	keys[3] = fmt.Sprintf(name, "std-dev", t, fmt.Sprintf("%.4f", h.StdDev()))
	keys[4] = fmt.Sprintf(name, "p50", t, fmt.Sprintf("%d", int64(ps[0])))
	keys[5] = fmt.Sprintf(name, "p75", t, fmt.Sprintf("%d", int64(ps[1])))
	keys[6] = fmt.Sprintf(name, "p95", t, fmt.Sprintf("%d", int64(ps[2])))
	keys[7] = fmt.Sprintf(name, "p99", t, fmt.Sprintf("%d", int64(ps[3])))
	keys[8] = fmt.Sprintf(name, "p999", t, fmt.Sprintf("%d", int64(ps[4])))
	keys[9] = fmt.Sprintf(name, "sample_size", t, fmt.Sprintf("%d", h.Sample().Size()))

	return keys
}

func (lg LogGroup) dataPoolHandler(channel_number int, tsd_pushers []chan []string, tsd_channel_number int) error {
	dataPool := make(map[string]*tsdPoint)
	tsd_push := tsd_pushers[tsd_channel_number]

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
							dataPool[data_point.name] = &tsdPoint{data: timemetrics.NewMeter(point_time, lg.interval),
								lastPush: point_time, lastCrunchedPush: point_time}
						default:
							log.Fatalf("Unexpected metric type %s!", data_point.metric_type)
						}
					}

					//Make sure data is ordered or we risk sending duplicate data
					if dataPool[data_point.name].lastPush.Unix() > point_time.Unix() {
						log.Printf("Non-ordered data detected in log file. Its key already had a update at %s in the future. Offending line: %s",
							dataPool[data_point.name].lastPush, data[0])
					}

					switch d := dataPool[data_point.name].data.(type) {
					case timemetrics.Histogram:
						d.Update(point_time, data_point.value)
					case timemetrics.Counter:
						d.Inc(point_time, data_point.value)
					case timemetrics.Meter:
						d.Mark(point_time, data_point.value)
					}
				}

				//Support for log playback - Push when <interval> has pass in the logs, not real time
				if point_time.Sub(*lastTimePushed) > (time.Duration(lg.interval) * time.Second) {
					//Update EWMAs
					for _, tsdPoint := range dataPool {
						switch v := tsdPoint.data.(type) {
						case timemetrics.Meter:
							sec_since_last_ewma_crunch := int(point_time.Unix() - v.GetMaxEWMATime().Unix())

							if sec_since_last_ewma_crunch > lg.ewma_interval {
								v.CrunchEWMA(point_time)
							}
						}
					}

					lastTimePushed = &point_time

					nbKeys := pushStats(tsd_push, dataPool)

					if lastNbKeys != nbKeys {
						log.Printf("Datapool[%s:%d] currently tracking %d keys", lg.name, channel_number, nbKeys)
					}

					lastNbKeys = nbKeys
				}
			}
		}
	}()

	return nil
}

func pushStats(tsd_push chan []string, dataPool map[string]*tsdPoint) (nbKeys int) {
	nbKeys = 0

	for tsd_key, tsdPoint := range dataPool {
		switch v := tsdPoint.data.(type) {
		case timemetrics.Histogram:
			snap := v.Snapshot()

			if snap.GetMaxTime().Unix() > tsdPoint.lastPush.Unix() { //Only push updated metrics
				tsdPoint.lastPush = snap.GetMaxTime()
				keys := getHistogramKeys(tsd_key, snap)
				tsd_push <- keys
			}

			nbKeys += 10
		case timemetrics.Counter:
			snap := v.Snapshot()
			if snap.GetMaxTime().Unix() > tsdPoint.lastPush.Unix() {
				tsd_push <- getCounterKeys(tsd_key, snap)
			}
			nbKeys += 1
		case timemetrics.Meter:

			if v.GetMaxTime().Unix() > tsdPoint.lastPush.Unix() {
				tsdPoint.lastPush = v.GetMaxTime()
				tsd_push <- getMeterKeyCount(tsd_key, v)
			}

			if v.GetMaxEWMATime().Unix() > tsdPoint.lastCrunchedPush.Unix() {
				tsdPoint.lastCrunchedPush = v.GetMaxEWMATime()
				tsd_push <- getMeterKeyRates(tsd_key, v)
			}

			nbKeys += 4
		}
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
