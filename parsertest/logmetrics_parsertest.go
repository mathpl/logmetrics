package main

import (
	"flag"
	//"log"
	"runtime"
	"syseng/logmetrics"
)

var configFile = flag.String("c", "/etc/logmetrics_collector.conf", "Full path to config file.")
var logGroup = flag.String("l", "", "Log group to test. (Default: all)")
var perfInfo = flag.Bool("p", false, "Print parser performance info. (Default: false)")
var threads = flag.Int("j", 1, "Thread count.")

func main() {
	//Process execution flags
	flag.Parse()

	//Set the number of real threads to start
	runtime.GOMAXPROCS(*threads)

	//Config
	config := logmetrics.LoadConfig(*configFile)

	//Start log parsing
	logmetrics.StartParserTest(&config, *logGroup, *perfInfo)
}
