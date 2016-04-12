package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/txn"
)

const txnsC = "txns"

func checkErr(label string, err error) {
	if err != nil {
		fmt.Println(label+":", err)
		os.Exit(1)
	}
}

type fakeLogger struct{}

func (l *fakeLogger) Output(_ int, s string) error {
	fmt.Println(s)
	return nil
}

func printUsageAndExit() {
	fmt.Println(`
usage: mgopurge <password> [collections...]

If no collections are specified, all of Juju's collections will be 
checked for orphaned transactions.
`[1:])
	os.Exit(1)
}

func processArgs(args []string) (password string, collections []string) {
	if len(args) < 1 {
		printUsageAndExit()
	}
	password = args[0]
	collections = args[1:]
	return
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

func main() {
	password, collections := processArgs(os.Args[1:])

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	dial := func(addr *mgo.ServerAddr) (net.Conn, error) {
		c, err := net.Dial("tcp", addr.String())
		checkErr("Dial (DialServer)", err)
		cc := tls.Client(c, tlsConfig)
		err = cc.Handshake()
		checkErr("Handshake", err)
		return cc, nil
	}
	info := &mgo.DialInfo{
		Addrs:      []string{"127.0.0.1:37017"},
		Database:   "admin",
		Username:   "admin",
		Password:   password,
		DialServer: dial,
	}
	session, err := mgo.DialWithInfo(info)
	checkErr("Dial", err)

	txn.SetLogger(&fakeLogger{})

	// If the user didn't specify collections on the command line,
	// inspect the DB to find them all.
	db := session.DB("juju")
	if len(collections) == 0 {
		collections = getAllPurgeableCollections(db)
	}

	runner := txn.NewRunner(db.C(txnsC))
	fmt.Printf("Purging orphaned transactions for: %s\n",
		strings.Join(collections, ", "))
	err = runner.PurgeMissing(collections...)
	checkErr("PurgeMissing", err)
	fmt.Println("Done!")
}
