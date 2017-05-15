// Copyright 2017 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	mgo "gopkg.in/mgo.v2"

	"github.com/juju/txn"
)

var dbName = flag.String("db", "", "mongo database name (required)")
var txnsName = flag.String("txns", "txns", "mgo txns collection name")
var url = flag.String("url", "localhost:27017", "mongo URL")
var insecureTLS = flag.Bool("insecuretls", false, "connect with insecure TLS handshake")
var dialTimeout = flag.Int("dialtimeout", 10, "dial timeout")
var syncTimeout = flag.Int("synctimeout", 7, "session sync timeout")
var socketTimeout = flag.Int("sockettimeout", 60, "session socket timeout")

func main() {
	flag.Usage = wrapUsage(flag.Usage)
	flag.Parse()

	if *dbName == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	var (
		session *mgo.Session
		err     error
	)

	if *insecureTLS {
		info, err := mgo.ParseURL(*url)
		if err != nil {
			log.Fatalf("failed to parse url: %v", err)
		}
		info.DialServer = dialInsecureTLS
		session, err = mgo.DialWithInfo(info)
		if err != nil {
			log.Fatalf("failed to connect to mongo: %v", err)
		}
	} else {
		session, err = mgo.DialWithTimeout(*url, time.Second*time.Duration(*dialTimeout))
		if err != nil {
			log.Fatalf("failed to connect to mongo: %v", err)
		}
	}
	session.SetSyncTimeout(time.Second * time.Duration(*syncTimeout))
	session.SetSocketTimeout(time.Second * time.Duration(*socketTimeout))

	db := session.DB(*dbName)
	txnsC := db.C(*txnsName)

	startTime := time.Now()
	stats, err := txn.CleanAndPrune(db, txnsC, 0)
	if err != nil {
		log.Fatalf("failed to clean and prune txns: %v", err)
	}

	log.Println("clean and prune complete after", time.Since(startTime))
	log.Println(stats.DocsCleaned, "docs cleaned,", stats.TransactionsRemoved, "txns removed,",
			    stats.StashDocumentsRemoved, "txns.stash docs removed")
}

func dialInsecureTLS(addr *mgo.ServerAddr) (net.Conn, error) {
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

func wrapUsage(f func()) func() {
	return func() {
		fmt.Fprintf(os.Stderr, `%s - clean and purge mgo/txn collections and affected documents

Do not use this utility unless you understand mgo/txn internals and
know what you are doing. Data loss may result from inappropriate or
incorrect usage. Good luck!

`, filepath.Base(os.Args[0]))
		f()
	}
}
