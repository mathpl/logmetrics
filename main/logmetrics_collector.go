package main

import (
	"flag"
	"log"
	"log/syslog"
	"os"
	"os/signal"
	"os/user"
	"runtime"
	"runtime/pprof"
	"strconv"
	"syscall"
	"syseng/logmetrics"
)

var configFile = flag.String("c", "/etc/logmetrics-collector.conf", "Full path to config file.")
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

	//Switch user
	if config.GetUser() != "" {
		if user, err := user.Lookup(config.GetUser()); err == nil {
			uid, _ := strconv.Atoi(user.Uid)
			if err = syscall.Setuid(uid); err != nil {
				log.Fatalf("Unable to change running user to %s: %s", config.GetUser(), err)
			}

			gid, _ := strconv.Atoi(user.Uid)
			if err = syscall.Setgid(gid); err != nil {
				log.Fatalf("Unable to change running user to %s: %s", config.GetUser(), err)
			}

			log.Printf("Changed to user %s (uid:%d gid:%d)", config.GetUser(), user.Uid, user.Gid)
		} else {
			log.Fatalf("Unable to change running user to %s: %s", config.GetUser(), err)
		}
	} else {
		log.Printf("No user setting, running as current user.")
	}

	os.Exit(0)

	//Logger
	logger, err := syslog.New(syslog.LOG_LOCAL3, "logmetrics_collector")
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Close()

	if !*logToConsole {
		log.SetOutput(logger)
	}

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
