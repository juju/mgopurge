package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/txn"
)

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

func main() {
	collections := os.Args[2:]

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
		Password:   os.Args[1],
		DialServer: dial,
	}
	session, err := mgo.DialWithInfo(info)
	checkErr("Dial", err)

	txn.SetLogger(&fakeLogger{})

	txns := session.DB("juju").C("txns")
	runner := txn.NewRunner(txns)
	err = runner.PurgeMissing(collections...)
	checkErr("PurgeMissing", err)
}
