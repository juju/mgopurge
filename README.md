# mgopurge

This is a tool to repair broken mgo/txn transaction references in a Juju MongoDB instance. It is only of interest to people responsible for Juju installations that have had some kind of catastrophic database corruption.

You need to run on it one of the Juju state server machines and the state server machine agents should be shut down. Make sure that the MongoDB replicaset is in a good state before running it.

You'll need to determine the password for Juju's MongoDB by looking in the machine agent's configuration file using the following command:

sudo grep oldpassword /var/lib/juju/agents/machine-*/agent.conf  | cut -d' ' -f2

Then run mgopurge like this:

./mgopurge <password>

If it fixes any problems you'll see output about "purging" as it performs the fixes.
