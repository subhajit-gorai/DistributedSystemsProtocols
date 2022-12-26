
## To run the code please type in the following command 
### for testing election_failure
```
 python3.9 -u raft_election_failure_test.py 5  python3.9 -u raft.py
```
###for testing election

```
 python3.9 -u raft_election_test.py 5  python3.9 -u raft.py
```
###for testing partition
```
 python3.9 -u raft_partition_test.py 5  python3.9 -u raft.py
```

##### Guidance For testing

The `pinger.py` script is a very simple user of the framework that simply sends a `PING` message to the next PID in a ring.

## Running tests

The tests take a single argument, _n_, which is the number of Raft processes to use. E.g.: `python3.10 raft_partiton_test.py 5`. Currently enabled tests:

* `raft_election_test`: tests simple election of a Raft leader: waits for the processes to elect a leader and for 
each other process to become a follower by receiving `AppendEntries`
* `raft_election_failure_test`: after a leader is elected, stops the leader and waits for the remaining processes to elect a new leader
* `raft_partition_test`: waits for a leader to be elected, then partitions that leader off from the rest of the group. Once the remaining group elects a new leader, it repairs the partition and waits for the previous leader to catch up
* `raft_simple_log_test`: after a leader is elected, requests the leader to log a message and waits for it to be committed on all servers
* `raft_log5_test`: logs 5 messages in a sequence
* `raft_log_leader_failure_test`: logs 5 messages, fails a leader, waits for re-election and logs 5 more
* `raft_log_follower_failure_test`: logs 5 messages, fails a minority of followers and logs 5 more, then fails another follower and makes sure that commits don't happen without a majority
* `raft_log_partition_test`: logs 5 messages, partitions off leader, logs 5 more with new leader, repairs partition and waits for old leader to catch up

