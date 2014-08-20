package main

import (
	"flag"
	"log"
	"log/syslog"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/mathpl/logmetrics"
)

var configFile = flag.String("c", "/etc/logmetrics_collector.conf", "Full path to config file.")
var threads = flag.Int("j", 1, "Thread count.")
var logToConsole = flag.Bool("d", false, "Print to console.")
var doNotSend = flag.Bool("D", false, "Print data instead of sending over network.")
var profile = flag.String("P", "", "Create a pprof file with this filename.")

func main() {
	//Process execution flags
	flag.Parse()

	var pf *os.File
	if *profile != "" {
		var err error
		pf, err = os.Create(*profile)
		if err != nil {
			log.Fatal(err)
		}

		log.Print("Starting profiler")
		pprof.StartCPUProfile(pf)
	}

	//Channel to stop the program
	stop := make(chan bool)

	//Signal handling
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		s := <-sigc
		log.Printf("Received signal: %s", s)
		stop <- true
	}()

	//Set the number of real threads to start
	runtime.GOMAXPROCS(*threads)

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
	fps := logmetrics.StartTails(&config, tsd_pushers)

	//Start datapools
	dps := logmetrics.StartDataPools(&config, tsd_pushers)

	//Start TSD pusher
	ps := logmetrics.StartTsdPushers(&config, tsd_pushers, *doNotSend)

	//Block until we're told to stop
	<-stop

	log.Print("Stopping all goroutines...")

	//Stop file checkers
	for _, fp := range fps {
		fp.Bye <- true
	}

	//Stop data pools
	for _, dp := range dps {
		dp.Bye <- true
	}

	//Stop tsd pushers
	for _, ps := range ps {
		ps.Bye <- true
	}

	if *profile != "" {
		pprof.StopCPUProfile()
		pf.Close()
		log.Print("Stopped profiler")
	}

	log.Print("All stopped")
}
