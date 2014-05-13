package main

import (
	"flag"
	"github.com/mathpl/logmetrics"
	"log"
	"log/syslog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

var configFile = flag.String("c", "/etc/logmetrics_collector.conf", "Full path to config file.")
var threads = flag.Int("j", 1, "Thread count.")
var logToConsole = flag.Bool("d", false, "Print to console.")
var doNotSend = flag.Bool("D", false, "Print data instead of sending over network.")

func main() {
	//Process execution flags
	flag.Parse()

	//Set the number of real threads to start
	runtime.GOMAXPROCS(*threads)

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

	//Logger
	logger, err := syslog.New(config.GetSyslogFacility(), "logmetrics_collector")
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Close()

	if !*logToConsole {
		log.SetOutput(logger)
	} else {
		log.SetFlags(0)
	}

	//Start the out channels
	tsd_pushers := make([]chan []string, config.GetPusherNumber())
	for i := 0; i < config.GetPusherNumber(); i++ {
		tsd_pushers[i] = make(chan []string, 1000)
	}

	//Start log tails
	logmetrics.StartTails(&config, tsd_pushers)

	//Start datapools
	logmetrics.StartDataPools(&config, tsd_pushers)

	//Start TSD pusher
	logmetrics.StartTsdPushers(&config, tsd_pushers, *doNotSend)

	//Block until we're told to stop
	<-stop
}
