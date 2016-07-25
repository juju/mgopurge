# mgopurge

This is a tool to repair broken mgo/txn transaction references in a
Juju MongoDB instance. It is only of interest to people responsible
for Juju installations that have had some kind of catastrophic
database corruption.

You need to run on it one of the Juju controller machines. All
controller machine agents must be shut down. Also ensure that the
MongoDB replicaset is in a good state before running it.

You'll need to determine the password for Juju's MongoDB by looking in
the machine agent's configuration file using the following command:

```
sudo grep oldpassword /var/lib/juju/agents/machine-*/agent.conf  | cut -d' ' -f2
```

Then run mgopurge like this:

```
./mgopurge --password <password>
```

By default mgopurge will attempt to connect to the port on localhost
where Juju's own MongoDB instance runs. There are also options to
support connecting to an arbitrary mongod. This is useful for running
against a Juju database has been dumped and restored into a temporary
MongoDB server.

A number of options are available for controlling which kinds of
repairs mgopurge will attempt (it will try them all by default).

See `./mgopurge --help` for further information.

## Building

This project uses [gb] to manage its dependencies. In order to build
mgopurge, install `gb` then run ` gb build`.

[gb]: https://getgb.io/
