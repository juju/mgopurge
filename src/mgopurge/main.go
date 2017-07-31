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

	jujutxn "github.com/juju/txn"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/txn"
)

const txnsC = "txns"
const txnsStashC = txnsC + ".stash"

// TODO (jam): 2017-07-07 Change the stages to take a settings parameter
// and move this into a local variable instead of a global variable.
var txnBatchSize = defaultTxnBatchSize

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
		"Trim txn-queues that are longer than 1000",
		func(db *mgo.Database, txns *mgo.Collection) error {
			collections := getAllPurgeableCollections(db)
			trimmer := &LongTxnTrimmer{
				timer:        newSimpleTimer(15 * time.Second),
				txns:         txns,
				longTxnSize:  1000,
				txnBatchSize: txnBatchSize,
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
			stats, err := jujutxn.CleanAndPrune(jujutxn.CleanAndPruneArgs{
				Txns:    txns,
				MaxTime: time.Now().Add(-time.Hour),
			})
			logger.Infof("clean and prune cleaned %d docs in %d collections\n"+
				"  removed %d transactions and %d stash documents",
				stats.DocsCleaned, stats.CollectionsInspected,
				stats.TransactionsRemoved, stats.StashDocumentsRemoved)
			return err
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
	flags.IntVar(&txnBatchSize, "txn-batch-size", defaultTxnBatchSize,
		"how many transactions to process at once, higher requires more memory but completes faster")
	var rawStages string
	flags.StringVar(&rawStages, "stages", "",
		"comma separated list of stages to run (default is to run all)")
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

	if rawStages == "" {
		// No stages selected. Run all.
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
