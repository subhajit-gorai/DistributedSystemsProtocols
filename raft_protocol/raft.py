#!/usr/bin/ python3.9

import json, threading, queue, os, sys, time
import math
from random import random
import logging
from dataclasses import dataclass
import dataclasses
import random
from threading import Timer
from utils import RaftState
import utils
from dacite import from_dict
import dacite
# from marshmallow_dataclass import dataclass
# import marshmallow_dataclass
# from marshmallow_dataclass import dataclass
# from marshmallow import Schema

import atexit

server = None
taskQueue= queue.Queue()

election_timer = None
heartbeat_timer = None
class RaftServer():
    def __init__(self, node_id, cluster):
        self.node_id = node_id
        self.cluster = cluster
        self.state = RaftState(node_id)
        self.printState()
        self.heartbeat_interval = 1
        # self.heartbeat_timer = None
        # self.election_timer = None
        self.restartElectionTimer()
        self.readData()
        # print(f"timer started Server {pid}", file=sys.stderr)

    def receivedVoteResponse(self, res):
        if res.term > self.state.current_term:
            self.state.leaderId = None
            self.state.term = res.term
            self.demoteToFollower()
        if self.state.status != RaftState.CANDIDATE:
            # either already the leader or have been demoted
            return

        if res.response:
            # print(f"got Yes from {res.sender} for {self.node_id} #votes{len(self.state.votes)}", file=sys.stderr)
            self.state.votes.add(res.sender)


        if len(self.state.votes) > int((self.cluster) // 2):
            # print(f"majority of votes granted {self.state.votes}, transitoning to leader {self.node_id}", file=sys.stderr)
            self.promoteToLeader()





    def send_AppendRPC(self):
        global heartbeat_timer
        if heartbeat_timer is not None:
            heartbeat_timer.cancel()
        for i in range(self.cluster):
            if i != self.node_id:
                append_rpc_request = self.sendAppendRPCRequest(self.node_id, i)
                self.sendToNode(append_rpc_request, i)
        heartbeat_timer = Timer(self.heartbeat_interval, self.send_AppendRPC)
        heartbeat_timer.start()

    def checkMajorityReplicationOfLog(self, check_log_idx):
        matched_values = sorted(self.state.match_index.values())
        max_log_record = -1
        for i in range(self.cluster):
            if i <= ((self.cluster-1) // 2):
               max_log_record = max(max_log_record, matched_values[i])
        if max_log_record >= check_log_idx:
            return True
        return False


    def sendAppendRPCRequest(self, sender, to):
        next_index = self.state.next_index[to] if to in self.state.next_index else 0
        prev_index = next_index - 1
        term = self.state.current_term #leder's term

        prev_term, _ = self.state.log[prev_index] if prev_index >= 0 else (-1, -1)

        # send all the entries follower's expect as per the Leader
        if len(self.state.log) - 1 >= next_index:
            entries = self.state.log[next_index:]
            entries = [[ent[0], ent[1]] for ent in entries]
        else:
            entries = []

        commit_index = self.state.commit_index
        appendRPCReq = utils.AppendEntriesRequestRPC(sender, term, prev_index, prev_term, entries, commit_index)
        return appendRPCReq

    def receivedAppendRPCRequest(self, req):

        if req.term < self.state.current_term:
            appendEntryResponse = utils.AppendEntriesResponseRPC(self.node_id, self.state.current_term, False,
                                                                 len(self.state.log) - 1)
            self.sendToNode(appendEntryResponse, req.sender)
            return

        #valid leader now
        if self.state.status != RaftState.FOLLOWER:
            self.demoteToFollower()
            self.printState()

        if req.term > self.state.current_term:
            self.state.current_term = req.term
            self.printState()

        if self.state.leaderId != req.sender:
            self.state.leaderId = req.sender
            self.printState()

        entries = [(ent[0], ent[1]) for ent in req.entries]
        self.restartElectionTimer()
        success = self.state.appendEntriesInLog(req.prev_index, req.prev_term, entries)
        if success:
            match_index = len(self.state.log) - 1
            # If leaderCommit > commitIndex, set commitIndex =min(leaderCommit, index of last new entry)
            if req.commit_index > self.state.commit_index:
                self.state.commit_index = min(req.commit_index, match_index)


        else:
            match_index = 0

        appendEntryRes = utils.AppendEntriesResponseRPC(self.node_id, self.state.current_term, success, match_index)
        self.sendToNode(appendEntryRes, req.sender)
        self.applyAnyCommits()


    def receivedAppendRPCResponse(self, res):

        if res.term > self.state.current_term:
            # self.printState()
            self.state.leaderId = None
            self.state.voted_for = None
            self.state.current_term = res.term

            if self.state.status != self.state.FOLLOWER:
                self.demoteToFollower()
                self.printState()
            # else : # demoteToFollower is already printing state
            # self.printState()

        if self.state.status != RaftState.LEADER:
            return

        if res.success:
            #channels are not FIFO, so taking max
            self.state.match_index[res.sender] = max(res.match_index, self.state.match_index[res.sender])
            self.state.next_index[res.sender] = self.state.match_index[res.sender] + 1
        else:
            # retry, after moving one index to the left
            #TODO if convergence is taking time -- send all the logs
            self.state.next_index[res.sender] = max(0, self.state.next_index[res.sender] - 1)

        # if the follower's log is behind, do not wait for the heartbeat,
        # if the follower's log does not match .. it request entire log from the leader
        if self.state.match_index[res.sender] != len(self.state.log) - 1:
            append_rpc_request = self.sendAppendRPCRequest(self.node_id, res.sender)
            self.sendToNode(append_rpc_request, res.sender)

        # check if you can update your commit index respective to this follower's match_index
        check_log_idx = self.state.match_index[res.sender]

        if (
            check_log_idx > -1
            and self.state.commit_index < check_log_idx
            and self.checkMajorityReplicationOfLog(check_log_idx)
            and self.state.log[check_log_idx][0] == self.state.current_term

        ):
            self.state.commit_index = self.state.match_index[res.sender]

        self.applyAnyCommits()


    def recievedVoteRequest(self, req):

        if req.term < self.state.current_term:
            vote_msg = utils.VoteResponseRPC(self.state.current_term, self.node_id, False)
            # print(f"{self.node_id} vote request from {req.candidate} granted=False (candidate term lower than self)", file=sys.stderr)
            self.sendToNode(vote_msg, req.candidate)
            return

        if req.term > self.state.current_term and self.state.status != RaftState.FOLLOWER:
            self.state.current_term = req.term
            self.state.leaderId = None
            self.demoteToFollower()

        my_last_log_index = len(self.state.log) - 1
        my_last_log_term, _ = self.state.log[my_last_log_index] if my_last_log_index >= 0 else (-1, -1)
        if (
            self.state.voted_for is None
            or self.state.voted_for == req.candidate #in case network is sending duplicate messages
            and (
                (req.last_log_index >= my_last_log_index and req.last_log_term == my_last_log_term)
                or (req.last_log_term > my_last_log_term)
            )
        ):
            vote_msg = utils.VoteResponseRPC(req.term, self.node_id, True)
            self.state.voted_for = req.candidate
            self.sendToNode(vote_msg, req.candidate)
            self.restartElectionTimer()
            return
        else:
            vote_msg = utils.VoteResponseRPC(self.state.current_term, self.node_id, False)
            self.sendToNode(vote_msg, req.candidate)

    def applyAnyCommits(self):
        #print any pending log commits
        while self.state.commit_index > self.state.last_applied:
            term, logMessage = self.state.log[self.state.last_applied + 1]
            logMessage = logMessage.strip()
            if (self.state.status == RaftState.LEADER): #if you are not leader, you don't have to print commit message
                print(f"COMMITTED {logMessage} {self.state.last_applied+2}",end='\n', flush=True)
            # term, msg = self.state.log[self.state.commit_index]
            # msg = msg.strip()
            # print(f"STATE log[{self.state.commit_index+1}]=[{term}, \"{msg}\"]", file=sys.stderr)
            # self.printSatetWithoutLogs()
            # print(f"STATE log[{self.state.last_applied+2}]=[{term}, \"{logMessage}\"]", end="\n", flush=True)
            # print(f"STATE commitIndex=\"{self.state.last_applied+2}\"", end="\n", flush=True)
            self.state.last_applied += 1
        self.printState()

    def handle_election_request(self):
        # print(f"haven't heard from leader or election failed; beginning election node_id : {self.node_id} term : {self.state.current_term} leaderId : {self.state.leaderId}", file=sys.stderr)
        # if self.state.status != RaftState.CANDIDATE:
            # var_leaderId = self.state.leaderId
            # if var_leaderId is None:
            #     var_leaderId = "None"
            # print(f"STATE leader=\"{var_leaderId}\"", end='\n', flush=True)
        self.state.becomeCandidate(self.node_id)
        self.runElection()
        self.printState()


    def promoteToLeader(self):
        global election_timer
        if election_timer is not None:
            election_timer.cancel()
        # print(f"Become leader: {self.node_id}", file=sys.stderr)
        self.state.leaderId = self.node_id
        self.state.becomeLeader(self.cluster)
        self.send_AppendRPC()
        self.printState()
        # TODO this needs to be removed
        # log_msg = "subhajitgorai"
        # taskQueue.put(json.dumps(dataclasses.asdict((utils.ClientLog(log_msg)))))


    def demoteToFollower(self):
        global heartbeat_timer
        # print(f"reverting to follower {self.node_id}", file=sys.stderr)
        if heartbeat_timer is not None:
            heartbeat_timer.cancel()
        self.state.becomeFollower()
        self.printState()
        self.restartElectionTimer()

    def toCLS(self, typeDcls, taskJson):
        if typeDcls == 1:
            return dacite.from_dict(data_class=utils.VoteRequestRPC, data=taskJson)
        elif typeDcls == 2:
            return dacite.from_dict(data_class=utils.VoteResponseRPC, data=taskJson)
        elif typeDcls == 3:
            return dacite.from_dict(data_class=utils.ClientLogRequestRPC, data=taskJson)
        elif typeDcls == 4:
            return dacite.from_dict(data_class=utils.AppendEntriesRequestRPC, data=taskJson)
        elif typeDcls == 5:
            return dacite.from_dict(data_class=utils.AppendEntriesResponseRPC, data=taskJson)

    def receivedClientLogRequest(self, req):
        if self.state.status != RaftState.LEADER: #if I am not the leader, I do not do anything
            return
        prev_index = len(self.state.log)-1
        prev_term, _ = self.state.log[prev_index] if prev_index >= 0 else (-1, -1)
        entries = [(self.state.current_term, req.rawLog)]
        #appending in leader will always be successful
        success = self.state.appendEntriesInLog(prev_index, prev_term, entries)
        #print(f"client request to leader {success} {len(self.state.log)}", file=sys.stderr)
        # update leader's match and next index
        match_index = len(self.state.log) - 1
        self.state.match_index[self.node_id] = match_index
        self.state.next_index[self.node_id] = match_index + 1


    def readData(self):
        while True:
            task = taskQueue.get()
            # print(f"task got is {task}", file=sys.stderr)
            # var = "$" in task
            # print(f"condition {var}", file=sys.stderr)
            # if "$" in task:
            taskDict = json.loads(task)
            msgType = taskDict['msgSignature']
            req = self.toCLS(msgType, taskDict)

            if msgType == 1: #
                self.recievedVoteRequest(req)
            elif msgType == 2:
                self.receivedVoteResponse(req)
            elif msgType == 3:  # client's log message
                self.receivedClientLogRequest(req)
            elif msgType == 4:
                self.receivedAppendRPCRequest(req)
            elif msgType == 5:
                self.receivedAppendRPCResponse(req)


    def printSatetWithoutLogs(self):
        # dclsJson = json.dumps(f"STATE term={var_term}")
        print(f"STATE term=\"{self.state.current_term}\"", end='\n', flush=True)
        print(f"STATE state=\"{self.state.status}\"", end='\n', flush=True)
        if self.state.leaderId != None:
            print(f"STATE leader=\"{self.state.leaderId}\"", end='\n', flush=True)
        else:
            print(f"STATE leader=null", end='\n', flush=True)

    def printState(self):
        # TODO need to print like this - as in message should be in double quotes
        # TODO STATE log[2]=[2, "qcs8SuEjiiJ2YnXJhIE4AJy65Jed3Xm65YkCIZaGugM"]
        # dclsJson = json.dumps(f"STATE term={var_term}")
        print(f"STATE term=\"{self.state.current_term}\"", end='\n', flush=True)
        print(f"STATE state=\"{self.state.status}\"", end='\n', flush=True)
        if self.state.leaderId != None:
            print(f"STATE leader=\"{self.state.leaderId}\"", end='\n', flush=True)
        else:
            print(f"STATE leader=null", end='\n', flush=True)
        if len(self.state.log) > 0:
            for loop in range(len(self.state.log)):
                term, msg = self.state.log[loop]
                msg = msg.strip()
                # print(f"STATE log[{self.state.commit_index+1}]=[{term}, \"{msg}\"]", file=sys.stderr)
                print(f"STATE log[{loop+1}]=[{term}, \"{msg}\"]", end="\n", flush=True)
        if self.state.commit_index >= 0:
            print(f"STATE commitIndex=\"{self.state.commit_index+1}\"", end="\n", flush=True)

    def sendToNode(self, datacls, i):
        if i == self.node_id:
            return
        dclsJson = json.dumps(dataclasses.asdict(datacls))
        print(f"SEND {i} ${dclsJson}", end='\n', flush=True)

    def sendToALL(self, datacls):
        dclsJson = json.dumps(dataclasses.asdict(datacls))
        for i in range(self.cluster):
            if i != self.node_id:
                print(f"SEND {i} ${dclsJson}", end='\n', flush=True)

    def runElection(self):

        my_last_log_index = len(self.state.log) - 1
        my_last_log_term, _ = self.state.log[my_last_log_index] if my_last_log_index >= 0 else (-1, -1)
        voteRPC = utils.VoteRequestRPC(self.state.current_term, self.node_id, my_last_log_index, my_last_log_term)
        self.sendToALL(voteRPC)
        self.restartElectionTimer()

    def restartElectionTimer(self):
        global election_timer
        # print(f"restart_election_timer started {self.node_id}", file=sys.stderr)
        if election_timer is not None:
            election_timer.cancel()
        base_interval = self.heartbeat_interval * 5
        interval = ((1.0 * base_interval) + (random.random() * base_interval))
        election_timer = Timer(interval, self.handle_election_request)
        election_timer.start()
        # print(f"restart_election_timer ended {self.node_id}", file=sys.stderr)

def runNode(pid, n):
    global server
    server = RaftServer(pid, n)


def addShutDownHook():
    global election_timer
    global heartbeat_timer
    # print("shut down hook attahced ", file=sys.stderr, flush=True)
    if heartbeat_timer is not None:
        heartbeat_timer.cancel()
    if election_timer is not None:
        election_timer.cancel()

    # print("shut down hook completed ", file=sys.stderr, flush=True)

if __name__ =="__main__":
    pid = int(sys.argv[1])
    n = int(sys.argv[2])
    atexit.register(addShutDownHook)
    threading.Thread(target=runNode, args=(pid, n)).start()
    while True:
        line = sys.stdin.readline()
        if line is None:
            break
        if "$" in line: #our communication
            taskQueue.put(line.split("$")[-1])
        elif "LOG" in line: #raw log given to us
            taskQueue.put(json.dumps(dataclasses.asdict((utils.ClientLogRequestRPC(line.split(" ")[-1])))))





# python3.10 -u framework.py 3 python3.10 -u raftOG.py
# python3.9 -u framework.py 3 python3.9 -u raft.py
# python3.9 -u raft_election_test.py 5 python3.9 -u raft.py
