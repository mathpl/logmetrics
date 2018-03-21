package logmetrics

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"log/syslog"
	"os"
	"strings"
	"time"

	"github.com/mathpl/golang-pkg-pcre/src/pkg/pcre"
	"launchpad.net/~niemeyer/goyaml/beta"
)

type Config struct {
	pollInterval   int
	pushPort       int
	pushWait       int
	pushHost       string
	pushProto      string
	pushType       string
	pushNumber     int
	stats_interval int
	logFacility    syslog.Priority

	logGroups map[string]*logGroup
}

//type match struct {
//	str     string
//	matcher *pcre.Regexp
//}
//
//func (m *match) apply(str string) (bool, string) {
//	result := m.matcher.MatcherString(str, 0)
//
//	return result.Matches(), ""
//}

type keyExtract struct {
	tag         string
	metric_type string
	key_suffix  string
	format      string
	never_stale bool
	multiply    int
	divide      int

	operations map[string][]int
}

type logGroup struct {
	name              string
	globFiles         []string
	filename_match    string
	filename_match_re *pcre.Regexp
	re                []*pcre.Regexp
	strRegexp         []string
	expected_matches  int
	hostname          string

	date_position int
	date_format   string

	key_prefix string
	tags       map[string]interface{}
	metrics    map[int][]keyExtract
	transform  map[int]transform

	histogram_size                  int
	histogram_alpha_decay           float64
	histogram_rescale_threshold_min int
	ewma_interval                   int
	stale_removal                   bool
	stale_treshold_min              int
	send_duplicates                 bool

	goroutines int
	interval   int
	poll_file  bool
	live_poll  bool

	fail_operation_warn    bool
	fail_regex_warn        bool
	out_of_order_time_warn bool
	log_stale_metrics      bool
	parse_from_start       bool

	//Channels
	tail_data []chan lineResult
}

func (lg *logGroup) getNbTags() int {
	return len(lg.tags)
}

func (lg *logGroup) getNbKeys() int {
	i := 0
	for _, metrics := range lg.metrics {
		i += len(metrics)
	}
	return i
}

func (conf *Config) GetPusherNumber() int {
	return conf.pushNumber
}

func (conf *Config) GetTsdTarget() string {
	return fmt.Sprintf("%s:%d", conf.pushHost, conf.pushPort)
}

func (conf *Config) GetSyslogFacility() syslog.Priority {
	return conf.logFacility
}

func (lg *logGroup) CreateDataPool(channel_number int, tsd_pushers []chan []string, tsd_channel_number int) *datapool {
	var dp datapool
	dp.Bye = make(chan bool)
	dp.duplicateSent = make(map[string]*time.Time)

	dp.channel_number = channel_number
	dp.tail_data = lg.tail_data[channel_number]

	dp.data = make(map[string]*tsdPoint)

	dp.tsd_channel_number = tsd_channel_number
	dp.tsd_push = tsd_pushers[tsd_channel_number]

	dp.last_time_file = make(map[string]fileInfo)

	dp.lg = lg

	dp.compileTagOrder()

	return &dp
}

func getHostname() string {
	//Get hostname
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Unable to get hostname: ", err)
	}

	return hostname
}

func cleanSre2(log_group_name string, re string) (string, *pcre.Regexp, error) {
	//Little hack to support extended style regex. Removes comments, spaces en endline
	noSpacesRe := strings.Replace(re, " ", "", -1)
	splitRe := strings.Split(noSpacesRe, "\\n")

	var rebuiltRe []string
	for _, l := range splitRe {
		noComments := strings.Split(l, "#")
		rebuiltRe = append(rebuiltRe, string(noComments[0]))
	}
	cleanRe := strings.Join(rebuiltRe, "")

	//Try to compile the regex
	if compiledRe, err := pcre.Compile(cleanRe, 0); err == nil {
		return cleanRe, &compiledRe, nil
	} else {
		return "", nil, errors.New(err.Message)
	}
}

func parseMetrics(conf map[interface{}]interface{}) map[int][]keyExtract {
	keyExtracts := make(map[int][]keyExtract)

	for metric_type, metrics := range conf {
		for _, n := range metrics.([]interface{}) {
			m := n.(map[interface{}]interface{})

			key_suffix := m["key_suffix"].(string)

			var format string
			var multiply int
			var divide int
			var never_stale bool

			if format_key, ok := m["format"]; ok == true {
				format = format_key.(string)
			} else {
				format = "int"
			}
			if multiply_key, ok := m["multiply"]; ok == true {
				multiply = multiply_key.(int)
				if multiply == 0 {
					log.Fatalf("A 'multiply' transform cannot be zero")
				}
			} else {
				multiply = 1
			}
			if divide_key, ok := m["divide"]; ok == true {
				divide = divide_key.(int)
				if divide < 1 {
					log.Fatalf("A 'divide' transform cannot be zero")
				}
			} else {
				divide = 1
			}
			if never_stale_key, ok := m["never_stale"]; ok == true {
				never_stale = never_stale_key.(bool)
			} else {
				never_stale = false
			}

			for _, val := range m["reference"].([]interface{}) {
				position := val.([]interface{})[0].(int)
				tag := val.([]interface{})[1].(string)

				operations := make(map[string][]int)
				if len(val.([]interface{})) > 2 {
					operations_struct := val.([]interface{})[2].(map[interface{}]interface{})

					for op, opvals := range operations_struct {
						//Make sure we only accept operation we can perform
						if op != "add" && op != "sub" {
							log.Fatalf("Operation %s no supported", op)
						}

						for _, opval := range opvals.([]interface{}) {
							operations[op.(string)] = append(operations[op.(string)], opval.(int))
						}
					}
				}

				newKey := keyExtract{tag: tag, metric_type: metric_type.(string), key_suffix: key_suffix,
					format: format, multiply: multiply, divide: divide, never_stale: never_stale, operations: operations}
				keyExtracts[position] = append(keyExtracts[position], newKey)
			}
		}
	}

	return keyExtracts
}

func LoadConfig(configFile string) Config {
	byteConfig, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	var rawCfg interface{}
	err = goyaml.Unmarshal(byteConfig, &rawCfg)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	settings := rawCfg.(map[interface{}]interface{})["settings"]

	var cfg Config
	cfg.logGroups = make(map[string]*logGroup)

	//Settings
	for key, val := range settings.(map[interface{}]interface{}) {
		switch v := val.(type) {
		case int:
			switch key {
			case "poll_interval":
				cfg.pollInterval = v
			case "push_port":
				cfg.pushPort = v
			case "push_wait":
				cfg.pushWait = v
			case "push_number":
				cfg.pushNumber = v
			case "stats_interval":
				cfg.stats_interval = v

			default:
				log.Fatalf("Unknown key settings.%s", key)
			}

		case string:
			switch key {
			case "log_facility":
				//Lookup
				if facility, found := facilityStrings[v]; found == true {
					cfg.logFacility = syslog.LOG_INFO | facility
				} else {
					log.Fatalf("Unable to map log_facility: %s", v)
				}
			case "push_host":
				cfg.pushHost = v
			case "push_proto":
				cfg.pushProto = v
			case "push_type":
				cfg.pushType = v

			default:
				log.Fatalf("Unknown key settings.%s", key)
			}

		default:
			log.Fatalf("Unknown key settings.%s", key)
		}
	}

	//Some default vals
	if cfg.pollInterval == 0 {
		cfg.pollInterval = 15
	}
	if cfg.logFacility == 0 {
		cfg.logFacility = syslog.LOG_LOCAL0
	}
	if cfg.pushHost == "" {
		cfg.pushHost = "localhost"
	}
	if cfg.pushProto == "" {
		cfg.pushProto = "udp"
	}
	if cfg.pushType == "" {
		cfg.pushType = "tcollector"
	}
	if cfg.pushNumber == 0 {
		cfg.pushNumber = 1
	}
	if cfg.stats_interval == 0 {
		cfg.stats_interval = 60
	}

	//Log_groups configs
	for name, group_content := range rawCfg.(map[interface{}]interface{}) {
		//Skip settings, already parsed
		if name == "settings" {
			continue
		}

		var lg logGroup

		lg.name = name.(string)
		lg.tags = make(map[string]interface{})

		//Process content
		for key, val := range group_content.(map[interface{}]interface{}) {
			switch v := val.(type) {
			case string:
				switch key {
				case "key_prefix":
					lg.key_prefix = v

				case "filename_match":
					lg.filename_match = v
					re := pcre.MustCompile(v, 0)
					lg.filename_match_re = &re

				default:
					log.Fatalf("Unknown key %s.%s", name, key)
				}

			case int:
				switch key {
				case "interval":
					lg.interval = v
				case "ewma_interval":
					lg.ewma_interval = v
				case "expected_matches":
					lg.expected_matches = v
				case "histogram_size":
					lg.histogram_size = v
				case "goroutines":
					lg.goroutines = v
				case "histogram_rescale_threshold_min":
					lg.histogram_rescale_threshold_min = v
				case "stale_treshold_min":
					lg.stale_treshold_min = v

				default:
					log.Fatalf("Unknown key %s.%s", name, key)
				}

			case float64:
				switch key {
				case "histogram_alpha_decay":
					lg.histogram_alpha_decay = v

				default:
					log.Fatalf("Unknown key %s.%s", name, key)
				}

			case bool:
				switch key {
				case "warn_on_regex_fail":
					lg.fail_regex_warn = v
				case "parse_from_start":
					lg.parse_from_start = v
				case "warn_on_operation_fail":
					lg.fail_operation_warn = v
				case "warn_on_out_of_order_time":
					lg.out_of_order_time_warn = v
				case "poll_file":
					lg.poll_file = v
				case "live_poll":
					lg.live_poll = v
				case "stale_removal":
					lg.stale_removal = v
				case "send_duplicates":
					lg.send_duplicates = v
				case "log_stale_metrics":
					lg.log_stale_metrics = v

				default:
					log.Fatalf("Unknown key %s.%s", name, key)
				}

			case []interface{}:
				switch key {
				case "re":
					var err error
					lg.re = make([]*pcre.Regexp, len(v))
					lg.strRegexp = make([]string, len(v))
					for i, re := range v {
						if lg.strRegexp[i], lg.re[i], err = cleanSre2(lg.name, re.(string)); err != nil {
							log.Fatal(err)
						}
					}
				case "files":
					for _, file := range v {
						lg.globFiles = append(lg.globFiles, file.(string))
					}

				default:
					log.Fatalf("Unknown key %s.%s", name, key)
				}

			case map[interface{}]interface{}:
				switch key {
				case "tags":
					for tag, pos := range v {
						switch pos.(type) {
						case int:
							lg.tags[tag.(string)] = pos.(int)
						case string:
							lg.tags[tag.(string)] = pos.(string)
						default:
							log.Fatalf("Unexpected type for tags section, key %s: %T", tag, pos)
						}
					}

				case "metrics":
					lg.metrics = parseMetrics(v)

				case "date":
					for date_name, date_val := range v {
						if date_name.(string) == "position" {
							lg.date_position = date_val.(int)
						} else if date_name.(string) == "format" {
							lg.date_format = date_val.(string)
						} else {
							log.Fatalf("Unknown key %s.date.%s", name, date_name)
						}
					}

				case "transform":
					lg.transform = parseTransform(v)

				default:
					log.Fatalf("Unknown key %s.%s", name, key)
				}

			default:
				log.Fatalf("Unknown key %s.%s", name, key)
			}
		}

		//Defaults
		if lg.goroutines == 0 {
			lg.goroutines = 1
		}
		if lg.histogram_alpha_decay == 0 {
			lg.histogram_alpha_decay = 0.15
		}
		if lg.histogram_size == 0 {
			lg.histogram_size = 256
		}
		if lg.histogram_rescale_threshold_min == 0 {
			lg.histogram_rescale_threshold_min = 60
		}
		if lg.ewma_interval == 0 {
			lg.ewma_interval = 30
		}
		if lg.stale_treshold_min == 0 {
			lg.stale_treshold_min = 60
		}

		//Init channels
		lg.tail_data = make([]chan lineResult, lg.goroutines)
		for i := 0; i < lg.goroutines; i++ {
			lg.tail_data[i] = make(chan lineResult, 1000)
		}
		lg.hostname = getHostname()

		cfg.logGroups[name.(string)] = &lg
	}

	return cfg
}
