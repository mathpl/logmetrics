package main

import (
	"flag"
	"log"
	"runtime"
	"syseng/logmetrics"
	//"time"
	"log/syslog"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
)

var configFile = flag.String("c", "/home/mpa/backlog/go/src/syseng/logmetrics/logmetrics-collector.conf", "Full path to config file.")
var threads = flag.Int("j", 1, "Thread count.")
var logToConsole = flag.Bool("d", false, "Print to console.")
var doNotSend = flag.Bool("D", false, "Do not send data out to TSD.")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	//Process execution flags
	flag.Parse()

	//Set the number of real threads to start
	runtime.GOMAXPROCS(*threads)

	//Enable cpu profiling if option is set
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	//Logger
	logger, err := syslog.New(syslog.LOG_LOCAL3, "logmetrics")
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Close()

	if !*logToConsole {
		log.SetOutput(logger)
	}

	//Channel to stop the program
	stop := make(chan bool)

	//Signal handling
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		s := <-sigc
		log.Printf("Received signal %s, stopping\n", s)
		stop <- true
	}()

	//Config
	config := logmetrics.LoadConfig(*configFile)

	//Start log tails
	logmetrics.StartTails(&config)

	//Start he out channels
	tsd_pushers := make([]chan []string, config.GetPusherNumber())
	for i := 0; i < config.GetPusherNumber(); i++ {
		tsd_pushers[i] = make(chan []string, 1000)
	}

	//Start datapools
	logmetrics.StartDataPools(&config, tsd_pushers)

	//Start TSD pusher
	logmetrics.StartTsdPushers(&config, tsd_pushers, *doNotSend)

	//Block until we're told to stop
	<-stop
}
