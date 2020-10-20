# mgopurge

This is a tool to repair broken mgo/txn transaction references in a
Juju MongoDB instance. It is only of interest to people responsible
for Juju installations that are experiencing catastrophic database
corruption. It should not be used casually. Improper use could lead to
irreverible damage to Juju deployments.

mgopurge is typically run on one of the Juju controller machines. All
controller machine agents must be shut down. Please ensure that the
MongoDB replicaset is in a good state before running it.

You'll need to determine the agent name and password for Juju's MongoDB
by looking in the machine agent's configuration file using the following command:

```
agent=$(cd /var/lib/juju/agents; echo machine-*)
pw=$(sudo grep statepassword /var/lib/juju/agents/${agent}/agent.conf | cut '-d ' -sf2)
```

Then run mgopurge like this:

```
./mgopurge --username $agent --password $pw
```

By default mgopurge will attempt to connect to the port on localhost
where Juju's own MongoDB instance runs. There are also options to
support connecting to an arbitrary mongod. This is useful for running
against a Juju database has been dumped and restored into a temporary
MongoDB server.

A number of options are available for controlling which kinds of
repairs mgopurge will attempt (it will perform them all by default).

See `./mgopurge --help` for further information.

## Building

This project uses [gb] to manage its dependencies. In order to build
mgopurge, install `gb` then run `make`. 

## Releases 

In order to build a release version:
* Ensure all changes are merged
* Create an annotated tag for the release, for example: `git tag -a v1.6 -m "1.6 release"`
* Run `make release` (the git tag will extracted and included in the binary)
* Check binary reports the expected version: `bin/mgopurge -version`
* Push the tag, for example: `git push origin v1.7`

Then create the release entry at https://github.com/juju/mgopurge/releases, uploading the binary you just built.

[gb]: https://getgb.io/
