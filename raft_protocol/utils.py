from dataclasses import dataclass
import dataclasses
import json
from dataclasses import field
# from marshmallow_dataclass import dataclass




@dataclass
class AppendEntriesResponseRPC:
    sender: int
    term: int
    success: bool
    match_index: int
    msgSignature: int = 5



@dataclass
class AppendEntriesRequestRPC:
    sender: int
    term: int
    prev_index: int
    prev_term: int
    entries: list
    commit_index: int
    msgSignature: int = 4


@dataclass
class VoteRequestRPC:
    term: int
    candidate: int
    last_log_index: int
    last_log_term: int
    msgSignature: int = 1

@dataclass
class VoteResponseRPC:
    term: int
    sender: int
    response: bool
    msgSignature: int = 2

@dataclass
class ClientLogRequestRPC:
    rawLog: str
    msgSignature: int = 3

class RaftState:
    
    LEADER = "LEADER"
    CANDIDATE = "CANDIDATE"
    FOLLOWER = "FOLLOWER"
    UNDECIDED = "UNDECIDED"

    def __init__(self, node_id):
        self.node_id = node_id
        self.status = self.FOLLOWER
        self.leaderId = None
        self.current_term = 0
        self.voted_for = None
        self.votesRecieved = set()
        self.log = []
        self.last_applied = -1
        self.match_index = {}
        self.commit_index = -1
        self.next_index = {}



    def appendEntriesInLog(self, prev_index, prev_term, entries):
        # can not append entry, since previous entries are not matching
        if prev_index >= len(self.log):
            return False
        # this is the first entry from the leader, add all the entries
        # after deleting any other uncommitted entry
        flag = False
        if prev_index == -1:
            flag = True
        # Reply True if log contains an entry at prev_index whose term matches prev_term
        elif len(self.log) > prev_index and self.log[prev_index][0] == prev_term:
            flag = True
        if flag:
            while len(self.log) > (prev_index + 1):
                self.log.pop()
            for entry in entries:
                self.log.append(entry)
        return flag


    def becomeLeader(self, cluster):
        self.match_index = {n: -1 for n in range(cluster)}  # reset the match index, since you don't know
        self.next_index = {n: len(self.log) for n in range(cluster)} # you first try to send all the logs
        self.status = self.LEADER
        self.leaderId = self.node_id

    def becomeFollower(self):
        self.status = self.FOLLOWER
        self.voted_for = None

    def becomeCandidate(self, node_id):
        self.leaderId = None
        self.status = self.CANDIDATE
        self.current_term += 1
        self.voted_for = node_id
        self.votes = set([node_id])