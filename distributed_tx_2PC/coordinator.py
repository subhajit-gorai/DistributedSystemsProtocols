import collections
import socket
import threading

import json, threading, queue, os, sys, time
import math
from random import random
import logging
from dataclasses import dataclass
import dataclasses
import random
from threading import Timer
from dacite import from_dict
import dacite
import util

class ClientThread():
    def __init__(self,coordinatorServer):
        self.coordinatorServer = coordinatorServer

class CoordinatorServer():
    def __init__(self):
        self.configFile = "config.txt"
        self.branchInfoDict = {}
        self.allAccountInfo = {} #{A.xyz : { balance = X , rts = int , wts = int } }
        self.setBranchInfoDict()

    def setBranchInfoDict(self):
        file = open(self.configFile, 'r')
        lines = file.readlines()
        for line in lines:
            words = line.split(" ")
            if len(words) == 3:
                self.branchInfoDict[words[0]] = {"host": words[1], "port": words[2]}
        file.close()

    def readValue(self, readRequest):
        branchName = readRequest.accountName.split("\\.")[0]
        if branchName in self.branchInfoDict:
            #need accountInfo bean, and requestType should be True - beacuse you are requesting for a read
            accountInfo = util.accountInfo(readRequest.branchName,readRequest.accountName,0, True, True)
            return self.sendToParticipant(branchName, accountInfo)

    def writeValue(self, writeRequest):
        branchName = writeRequest.accountName.split("\\.")[0]
        if branchName in self.branchInfoDict:
            # requestType should be false because you are writing value
            accountInfo = util.accountInfo(writeRequest.branchName,writeRequest.accountName, writeRequest.balance, False, True)
            self.sendToParticipant(branchName, accountInfo)

    def sendToParticipant(self, branchName, accountInfo):
        requestData = json.dumps(dataclasses.asdict(accountInfo))
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # TODO set appropriate timeout
                s.settimeout(2)
                print("The content send to server is " + requestData)
                #TODO add HostName for the branch
                s.connect(('', self.branchInfoDict[branchName]["port"]))
                s.send(requestData.encode(self.encoding))
                response = s.recv(2048).decode(self.encoding)
                responseDict = json.loads(response)
                responseObject = util.toCLS(responseDict)
                s.close()
                return responseObject
        except socket.timeout as err:
            print('There is a timeout in Client !! ', file=sys.stderr)
            print(err)
        except Exception:
            print('the error', file=sys.stderr)
        finally:
            s.close()
        return None