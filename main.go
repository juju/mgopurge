// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

//go:generate bash -c "./gen_version.sh > version.go"

package main

import (
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/juju/mgo/v2"
	"github.com/juju/mgo/v2/txn"
	jujutxn "github.com/juju/txn"
)

const txnsC = "txns"
const txnsStashC = txnsC + ".stash"
const defaultMaxTxnsToProcess = 1 * 1000 * 1000
const defaultPruneTxnBatchSize = 1000
const defaultTrimQueueLength = 500

// TODO (jam): 2017-07-07 Change the stages to take a settings parameter
// and move this into a local variable instead of a global variable.
var txnBatchSize = defaultTxnBatchSize
var pruneTxnBatchSize = defaultPruneTxnBatchSize
var trimQueueLength = defaultTrimQueueLength
var pruneSleepTimeMs = 0
var maxTxnsToProcess = defaultMaxTxnsToProcess
var multithreaded = true

var controllerPrompt = `
This program should only be used to recover from specific transaction
related problems in a Juju database. Casual use is strongly
discouraged. Irreversible damage may be caused to a Juju deployment
through improper use of this tool.

Aside from limited cases, this program should not be run while Juju
controller machine agents are running.

Ok to proceed?`[1:]

// allStages defines all of mgopurge's stages. As some stages must be
// run before others, the ordering is important.
var allStages = []stage{
	{
		"presence",
		"Clear presence data. This will be recreated when the controllers restart.",
		func(db *mgo.Database, _ *mgo.Collection) error {
			presenceDB := db.Session.DB("presence")
			err := presenceDB.DropDatabase()
			return err
		},
	}, {
		"purgemissing",
		"Purge orphaned transactions",
		func(db *mgo.Database, txns *mgo.Collection) error {
			collections := getAllPurgeableCollections(db)
			return PurgeMissing(txns, db.C(txnsStashC), collections...)
		},
	}, {
		"trim",
		"Trim txn-queues that are longer than trim-queue-length",
		func(db *mgo.Database, txns *mgo.Collection) error {
			collections := getAllPurgeableCollections(db)
			trimmer := &LongTxnTrimmer{
				timer:        newSimpleTimer(15 * time.Second),
				txns:         txns,
				longTxnSize:  trimQueueLength,
				txnBatchSize: txnBatchSize,
				txnsStash:    db.C(txnsStashC),
			}
			return trimmer.Trim(collections)
		},
	}, {
		"resume",
		"Resume incomplete transactions",
		func(db *mgo.Database, txns *mgo.Collection) error {
			return ResumeAll(txns)
		},
	}, {
		"prune",
		"Prune finalised transactions",
		func(db *mgo.Database, txns *mgo.Collection) error {
			first := true
			var totalStats jujutxn.CleanupStats
			totalStart := time.Now()
			for {
				phaseStart := time.Now()
				stats, err := jujutxn.CleanAndPrune(jujutxn.CleanAndPruneArgs{
					Txns:                     txns,
					MaxTime:                  time.Now().Add(-time.Hour),
					MaxTransactionsToProcess: maxTxnsToProcess,
					Multithreaded:            multithreaded,
					TxnBatchSize:             pruneTxnBatchSize,
					TxnBatchSleepTime:        time.Duration(pruneSleepTimeMs) * time.Millisecond,
				})
				logger.Infof("clean and prune cleaned %d docs in %d collections\n"+
					"  removed %d transactions and %d stash documents in %s",
					stats.DocsCleaned, stats.CollectionsInspected,
					stats.TransactionsRemoved, stats.StashDocumentsRemoved,
					time.Since(phaseStart).Round(time.Millisecond))
				if first {
					totalStats = stats
					first = false
				} else {
					totalStats.DocsCleaned += stats.DocsCleaned
					totalStats.CollectionsInspected += stats.CollectionsInspected
					totalStats.TransactionsRemoved += stats.TransactionsRemoved
					totalStats.StashDocumentsRemoved += stats.StashDocumentsRemoved
					logger.Infof("total clean and prune cleaned %d docs in %d collections\n"+
						"  removed %d transactions and %d stash documents in %s",
						totalStats.DocsCleaned, totalStats.CollectionsInspected,
						totalStats.TransactionsRemoved, totalStats.StashDocumentsRemoved,
						time.Since(totalStart).Round(time.Millisecond))
				}
				if err != nil {
					return err
				}
				if stats.ShouldRetry {
					logger.Infof("Not all transactions processed, running another clean step")
				} else {
					break
				}
			}
			return nil
		},
	}, {
		"compact",
		"Compact database to release disk space (does not compact replicas)",
		func(db *mgo.Database, _ *mgo.Collection) error {
			err := compact(db)
			if err != nil {
				return err
			}
			logsDB := db.Session.DB("logs")
			err = compact(logsDB)
			if err != nil {
				return err
			}
			return nil
		},
	},
}

func main() {
	checkErr("setupLogging", setupLogging())
	args := commandLine()

	if args.doPrompt && !promptYN(controllerPrompt) {
		return
	}

	session, err := dial(args)
	checkErr("Dial", err)

	db := session.DB("juju")
	txns := db.C(txnsC)
	for _, stage := range args.stages {
		stage.Run(db, txns)
	}
}

type stage struct {
	label string
	desc  string
	run   func(*mgo.Database, *mgo.Collection) error
}

func (s *stage) Run(db *mgo.Database, txns *mgo.Collection) {
	logger.Infof("Running stage %q: %s", s.label, s.desc)
	err := s.run(db, txns)
	checkErr("failed stage "+s.label, err)
}

func promptYN(question string) bool {
	fmt.Printf("%s [y/n] ", question)
	os.Stdout.Sync()
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return false
	}
	switch strings.ToLower(scanner.Text()) {
	case "y", "yes":
		return true
	default:
		return false
	}
}

func printAndFlush(s string) {
	fmt.Print(s)
	os.Stdout.Sync()
}

func dial(args commandLineArgs) (*mgo.Session, error) {
	info := &mgo.DialInfo{
		Addrs: []string{net.JoinHostPort(args.hostname, args.port)},
	}
	if args.username != "" {
		info.Database = "admin"
		info.Username = args.username
		info.Password = args.password
	}
	if args.ssl {
		info.DialServer = dialSSL
	}
	session, err := mgo.DialWithInfo(info)
	if err != nil {
		return nil, err
	}
	return session, nil
}

func dialSSL(addr *mgo.ServerAddr) (net.Conn, error) {
	c, err := net.Dial("tcp", addr.String())
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	cc := tls.Client(c, tlsConfig)
	if err := cc.Handshake(); err != nil {
		return nil, err
	}
	return cc, nil
}

func checkErr(label string, err error) {
	if err != nil {
		logger.Errorf("%s: %s", label, err)
		os.Exit(1)
	}
}

type commandLineArgs struct {
	hostname string
	port     string
	ssl      bool
	username string
	password string
	stages   []stage
	doPrompt bool
}

func commandLine() commandLineArgs {
	flags := flag.NewFlagSet("mgopurge", flag.ExitOnError)
	var a commandLineArgs
	flags.StringVar(&a.hostname, "hostname", "localhost",
		"hostname of the Juju MongoDB server")
	flags.StringVar(&a.port, "port", "37017",
		"port of the Juju MongoDB server")
	flags.BoolVar(&a.ssl, "ssl", true,
		"use SSL to connect to MonogDB ")
	flags.StringVar(&a.username, "username", "admin",
		"user for connecting to MonogDB (use \"\" to for no authentication)")
	flags.StringVar(&a.password, "password", "",
		"password for connecting to MonogDB")
	flags.StringVar(&loggingConfig, "logging-config", defaultLogConfig,
		"set the log levels of various modules")
	flags.IntVar(&txnBatchSize, "txn-batch-size", defaultTxnBatchSize,
		"how many transactions to prune at once, higher requires more memory but completes faster")
	flags.IntVar(&trimQueueLength, "trim-queue-length", defaultTrimQueueLength,
		"how long to leave transaction queues after the 'trim' stage")
	flags.IntVar(&maxTxnsToProcess, "max-txns", defaultMaxTxnsToProcess,
		"(deprecated and ignored) we used to process txns in large batches, we now do small batches, see prune-batch-size")
	flags.BoolVar(&multithreaded, "multithreaded", true,
		"by default mgopurge will run multiple prune passes in parallel, set to false to disable")
	flags.IntVar(&pruneTxnBatchSize, "prune-batch-size", defaultPruneTxnBatchSize,
		"during 'prune' process this many transactions together")
	flags.IntVar(&pruneSleepTimeMs, "prune-batch-sleep-ms", 0,
		"during 'prune' sleep this long between batches (reduces load)")
	var rawStages string
	flags.StringVar(&rawStages, "stages", "",
		"comma separated list of stages to run (default is to run all)")
	var skipStages string
	flags.StringVar(&skipStages, "skip-stages", "",
		"comma separated list of stages to skip (default is to run all)")
	listStagesFlag := flags.Bool("list", false, "list available execution stages")
	yes := flags.Bool("yes", false, "answer 'yes' to prompts")
	showVersion := flags.Bool("version", false, "show version")

	flags.Parse(os.Args[1:])

	if *showVersion {
		fmt.Fprintf(os.Stderr, "%s\n", version)
		os.Exit(0)
	}

	if *listStagesFlag {
		listStages()
		os.Exit(0)
	}

	if a.password == "" && a.username != "" {
		fmt.Fprintf(os.Stderr, "error: -password must be used if username is provided\n")
		os.Exit(2)
	}
	a.doPrompt = !*yes

	if skipStages != "" {
		if rawStages != "" {
			fmt.Fprintf(os.Stderr, "you cannot supply both --stages and --skip-stages\n")
			os.Exit(2)
		}
		skipped := make(map[string]bool)
		for _, s := range strings.Split(skipStages, ",") {
			skipped[strings.TrimSpace(s)] = true
		}
		for _, stage := range allStages {
			if !skipped[stage.label] {
				a.stages = append(a.stages, stage)
				delete(skipped, stage.label)
			}
		}
		if len(skipped) > 0 {
			var invalid []string
			for s := range skipped {
				invalid = append(invalid, s)
			}
			fmt.Fprintf(os.Stderr, "error: invalid stages to be skipped: %s\n",
				strings.Join(invalid, ","))
			os.Exit(2)
		}
	} else if rawStages == "" {
		// No stages selected. Run all
		for _, stage := range allStages {
			a.stages = append(a.stages, stage)
		}
	} else {
		// Specific stages selected.
		selected := make(map[string]bool)
		for _, s := range strings.Split(rawStages, ",") {
			selected[strings.TrimSpace(s)] = true
		}
		for _, stage := range allStages {
			if selected[stage.label] {
				a.stages = append(a.stages, stage)
				delete(selected, stage.label)
			}
		}
		if len(selected) > 0 {
			// There were invalid stages selected
			var invalid []string
			for s := range selected {
				invalid = append(invalid, s)
			}
			fmt.Fprintf(os.Stderr, "error: invalid stages selected: %s\n", strings.Join(invalid, ","))
			os.Exit(2)
		}
	}

	torun := make([]string, len(a.stages))
	for i, st := range a.stages {
		torun[i] = st.label
	}

	fmt.Fprintf(os.Stderr, "running stages: %v\n",torun)
	os.Exit(1)
	return a
}

func listStages() {
	for _, stage := range allStages {
		fmt.Printf("%s: %s\n", stage.label, stage.desc)
	}
}

func isPurgeableCollection(name string) bool {
	if name == txnsC {
		return false
	}
	if strings.HasPrefix(name, "system.") {
		return false
	}
	if strings.HasPrefix(name, txnsC+".") {
		return false
	}
	return true
}

func getAllPurgeableCollections(db *mgo.Database) (collections []string) {
	allCollections, err := db.CollectionNames()
	checkErr("CollectionNames", err)
	for _, collection := range allCollections {
		if isPurgeableCollection(collection) {
			collections = append(collections, collection)
		}
	}
	return
}

func ResumeAll(tc *mgo.Collection) error {
	runner := txn.NewRunner(tc)
	return runner.ResumeAll()
}
