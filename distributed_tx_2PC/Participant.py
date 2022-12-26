import collections
import socket
from threading import Thread

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


def getPortNumber(configFile, branchName):
    file = open(configFile, 'r')
    lines = file.readlines()
    portNumber = None
    for line in lines:
        words = line.split(" ")
        if words[0] == branchName:
            portNumber = int(words[2])
    file.close()
    return portNumber

class Coordinator():
    def __init__(self, participantServer):
        self.participantServer = participantServer
        self.allAccountInfo = {}  # {A.xyz : { balance = X , rts = int , wts = int } }
        self.transactionsReadVariable = {}  # {A.xyz : { balance = X , rts = int , wts = int } }
        self.transactionDepDict = {}
        self.transactionState = {}
        self.exhaustiveTransactions = {}
        self.readExhaustiveTransactions = {}
        self.branchInfoDict = {}
        self.setBranchInfoDict()

    def setBranchInfoDict(self):
        file = open(self.participantServer.configFile, 'r')
        lines = file.readlines()
        for line in lines:
            words = line.split(" ")
            if len(words) == 3:
                self.branchInfoDict[words[0]] = {"host": words[1], "port": int(words[2])}
        file.close()

    def sendToParticipant(self, branchName, accountInfo):
        requestData = json.dumps(dataclasses.asdict(accountInfo))
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # TODO set appropriate timeout
                # s.settimeout(5)
                # print("The content send to server is " + requestData)
                # print("The port number is", self.branchInfoDict[branchName]["port"])
                #TODO add HostName for the branch
                s.connect((self.branchInfoDict[branchName]["host"], self.branchInfoDict[branchName]["port"]))
                s.send(requestData.encode(self.participantServer.encoding))
                response = s.recv(2048).decode(self.participantServer.encoding)
                responseDict = json.loads(response)
                responseObject = util.toCLS(responseDict)
                return responseObject
        except socket.timeout as err:
            # print('There is a timeout in Client !! ', file=sys.stderr)
            # print(err)
            pass
        except Exception as err:
            # print('the error', file=sys.stderr)
            # print(err)
            pass
        finally:
            s.close()
        return None

    def rollBack(self, transactionTime): # assuming there is no collision when considering nanosecond
        if transactionTime in self.exhaustiveTransactions:
            while len(self.exhaustiveTransactions[transactionTime]) > 0:
                writeRollback = self.exhaustiveTransactions[transactionTime][-1]
                if writeRollback.accountName in self.allAccountInfo:
                    self.allAccountInfo[writeRollback.accountName]["balance"] = writeRollback.oldBalance
                    self.allAccountInfo[writeRollback.accountName]["wts"] = writeRollback.previousWTS
                self.exhaustiveTransactions[transactionTime].pop()

        if transactionTime in self.readExhaustiveTransactions:
            while len(self.readExhaustiveTransactions[transactionTime]) > 0:
                readRollback = self.readExhaustiveTransactions[transactionTime][-1]
                if readRollback.accountName in self.allAccountInfo:
                    # print("the previous RTS is ", readRollback.previousRTS)
                    self.allAccountInfo[readRollback.accountName]["rts"] = readRollback.previousRTS
                self.readExhaustiveTransactions[transactionTime].pop()

        if transactionTime in self.transactionsReadVariable:
            for accountName in self.transactionsReadVariable[transactionTime]:
                # print(f" the transactionsReadVariable has been dropped {accountName}")
                self.allAccountInfo.pop(accountName)
        # print(f"the entire account is {self.allAccountInfo}")

    def abortTransaction(self, transactionTime):
        self.transactionState[transactionTime] = "ABORTED"
        self.rollBack(transactionTime)

    def receivedAbortTransaction(self, abortTransaction):
        self.abortTransaction(abortTransaction.transactionTime)
        return util.abortResponse(True)

    def isFinalWriteNegative(self, transactionTime):
        accounts = {}
        if transactionTime in self.exhaustiveTransactions and len(self.exhaustiveTransactions[transactionTime]) > 0:
            for writeRollback in reversed(self.exhaustiveTransactions[transactionTime]):
                if writeRollback.accountName not in accounts:
                    accounts[writeRollback.accountName] = writeRollback.newBalance
                    if writeRollback.newBalance < 0 :
                        return True
        return False

    def receivedCommitTransaction(self,commitTransaction):
        #might have to wait till the dependency list's transaction is complete
        while True:
            try:
                if commitTransaction.transactionTime in self.transactionState and self.transactionState[commitTransaction.transactionTime] == "ABORTED":
                    return util.commitResponse(False)
                if commitTransaction.transactionTime in self.transactionState and self.transactionState[commitTransaction.transactionTime] == "OK":
                    return util.commitResponse(True)

                if self.isFinalWriteNegative(commitTransaction.transactionTime):
                    self.abortTransaction(commitTransaction.transactionTime)
                    return util.commitResponse(False)

                if commitTransaction.transactionTime not in self.transactionDepDict or len(self.transactionDepDict[commitTransaction.transactionTime]) == 0:
                    self.transactionState[commitTransaction.transactionTime] = "OK"
                    self.applyAllWrites(commitTransaction.transactionTime)
                    return util.commitResponse(True)
                # print(f"inside commit {self.transactionDepDict}")
                # print(f"all the states are{self.transactionState}")

                okCount = 0
                if commitTransaction.transactionTime in self.transactionDepDict:
                    for deps in self.transactionDepDict[commitTransaction.transactionTime]:
                        if deps in self.transactionState and self.transactionState[deps] == "ABORTED":
                            self.abortTransaction(commitTransaction.transactionTime)
                            return util.commitResponse(False)
                        elif deps in self.transactionState and self.transactionState[deps] == "OK":
                            okCount += 1
                # print(f"okCount = {okCount} and length of dependencies = {len(self.transactionDepDict[commitTransaction.transactionTime])}")
                if okCount == len(self.transactionDepDict[commitTransaction.transactionTime]):
                    self.transactionState[commitTransaction.transactionTime] = "OK"
                    self.applyAllWrites(commitTransaction.transactionTime)
                    return util.commitResponse(True)
                time.sleep(5) # sleep for 1 second
            except Exception as err:
                # print(err)
                pass

    def receivedRollbackUndefinedRead(self, rollbackUndefinedRead):
        if rollbackUndefinedRead.accountName in self.allAccountInfo:
            self.allAccountInfo.pop(rollbackUndefinedRead.accountName)
        return rollbackUndefinedRead


    def receivedReadRequest(self, readRequest):
        # print(f"before coordinator read request {self.allAccountInfo}")
        if readRequest.transactionTime in self.transactionState and self.transactionState[readRequest.transactionTime] == "ABORTED":
            return util.accountInfo(readRequest.branchName,readRequest.accountName,-1,True,False)

        noAccount = False
        if readRequest.accountName not in self.allAccountInfo:
            # print("Reading the value again")
            accountInfo = self.readValue(readRequest)
            if readRequest.transactionTime not in self.transactionsReadVariable:
                self.transactionsReadVariable[readRequest.transactionTime] = []
            self.transactionsReadVariable[readRequest.transactionTime].append(accountInfo.accountName)

            if accountInfo.success:
                accountDict = {} # {A.xyz : { balance = X , rts = int , wts = int } }
                accountDict["balance"] = accountInfo.balance
                accountDict["rts"] = 0
                accountDict["wts"] = 0
                self.allAccountInfo[readRequest.accountName] = accountDict

            else: #illegal transaction, abort it and return from here
                #self.abortTransaction(readRequest.transactionTime)
                # do not abort, instead add balance as 0-- for first time deposit case
                noAccount = True
                accountDict = {}  # {A.xyz : { balance = X , rts = int , wts = int } }
                accountDict["balance"] = 0
                accountDict["rts"] = 0
                accountDict["wts"] = 0
                self.allAccountInfo[readRequest.accountName] = accountDict
                # return util.accountInfo(readRequest.branchName, readRequest.accountName, -1, True, False)

        if self.allAccountInfo[readRequest.accountName]["wts"] > readRequest.transactionTime:
            self.abortTransaction(readRequest.transactionTime)
            return util.accountInfo(readRequest.branchName, readRequest.accountName, -1, True, False)

        if self.allAccountInfo[readRequest.accountName]["wts"] != 0 and self.allAccountInfo[readRequest.accountName]["wts"] < readRequest.transactionTime:

            if readRequest.transactionTime not in self.transactionDepDict:
                self.transactionDepDict[readRequest.transactionTime] = []
            if self.allAccountInfo[readRequest.accountName]["wts"] not in self.transactionDepDict[readRequest.transactionTime]:
                self.transactionDepDict[readRequest.transactionTime].append(self.allAccountInfo[readRequest.accountName]["wts"])
        readRollback = util.readRollback(readRequest.clientId, readRequest.transactionTime,readRequest.branchName,readRequest.accountName,self.allAccountInfo[readRequest.accountName]["rts"])
        if readRequest.transactionTime not in self.readExhaustiveTransactions:
            self.readExhaustiveTransactions[readRequest.transactionTime] = []
        if readRollback not in self.readExhaustiveTransactions[readRequest.transactionTime]:
            self.readExhaustiveTransactions[readRequest.transactionTime].append(readRollback)

        self.allAccountInfo[readRequest.accountName]["rts"] = max(readRequest.transactionTime, self.allAccountInfo[readRequest.accountName]["rts"])
        # print(f"after coordinator read request {self.allAccountInfo}")
        return util.accountInfo(readRequest.branchName, readRequest.accountName, self.allAccountInfo[readRequest.accountName]["balance"], True, True,noAccount)


    def receivedWriteRequest(self, writeRequest):
        # print(f"before coordinator write request {self.allAccountInfo}")
        if writeRequest.transactionTime in self.transactionState and self.transactionState[writeRequest.transactionTime] == "ABORTED":
            return util.accountInfo(writeRequest.branchName, writeRequest.accountName, -1, False, False)
        if writeRequest.accountName not in self.allAccountInfo:
            # print("error !! happened how come write request, if there is no previous read request !! ")
            self.abortTransaction(writeRequest.transactionTime)
            return util.accountInfo(writeRequest.branchName, writeRequest.accountName, -1, False, False)

        if self.allAccountInfo[writeRequest.accountName]["rts"] > writeRequest.transactionTime:
           self.abortTransaction(writeRequest.transactionTime)
           return util.accountInfo(writeRequest.branchName, writeRequest.accountName, -1, False, False)
        if self.allAccountInfo[writeRequest.accountName]["wts"] > writeRequest.transactionTime:
            #skip this write
            return util.accountInfo(writeRequest.branchName, writeRequest.accountName, self.allAccountInfo[writeRequest.accountName]["balance"], False, True)

        writeRollback = util.writeRollback(
            writeRequest.clientId,
            writeRequest.transactionTime,
            writeRequest.branchName,
            writeRequest.accountName,
            self.allAccountInfo[writeRequest.accountName]["balance"],
            writeRequest.balance,
            self.allAccountInfo[writeRequest.accountName]["wts"]
        )
        self.allAccountInfo[writeRequest.accountName]["wts"] = max(self.allAccountInfo[writeRequest.accountName]["wts"], writeRequest.transactionTime)
        self.allAccountInfo[writeRequest.accountName]["balance"] = writeRequest.balance

        if writeRequest.transactionTime not in self.exhaustiveTransactions:
            self.exhaustiveTransactions[writeRequest.transactionTime] = []
        if writeRollback not in self.exhaustiveTransactions[writeRequest.transactionTime]:
            self.exhaustiveTransactions[writeRequest.transactionTime].append(writeRollback)
        # print(f"after coordinator write request {self.allAccountInfo}")
        return util.accountInfo(writeRequest.branchName,writeRequest.accountName,writeRequest.balance,False,True)



    def readValue(self, readRequest):
        if readRequest.accountName in self.allAccountInfo:
            return
        branchName = readRequest.accountName.split(".")[0]
        # need accountInfo bean, and requestType should be True - beacuse you are requesting for a read
        accountInfo = util.accountInfo(readRequest.branchName, readRequest.accountName, 0, True, True)
        # if i have the information
        if branchName == self.participantServer.branchName:
            return self.participantServer.returnBalance(accountInfo)

        if branchName in self.branchInfoDict:
            return self.sendToParticipant(branchName, accountInfo)

    def writeValue(self, writeRequest):
        branchName = writeRequest.accountName.split(".")[0]
        # requestType should be false because you are writing value
        accountInfo = util.accountInfo(writeRequest.branchName, writeRequest.accountName, writeRequest.balance, False,True)
        if branchName == self.participantServer.branchName:
            self.participantServer.updateBalance(accountInfo)
            return
        if branchName in self.branchInfoDict:
            self.sendToParticipant(branchName, accountInfo)

    def applyAllWrites(self, transactionTime):
        accounts = {}
        branches = {}
        if transactionTime in self.exhaustiveTransactions and len(self.exhaustiveTransactions[transactionTime]) > 0:
            for writeRollback in reversed(self.exhaustiveTransactions[transactionTime]):
                if writeRollback.accountName not in accounts:
                    accounts[writeRollback.accountName] = writeRollback.newBalance
                    writeRequest = util.writeRequest(writeRollback.clientId,writeRollback.transactionTime,writeRollback.branchName,writeRollback.accountName,writeRollback.newBalance)
                    self.writeValue(writeRequest)
                    branches[writeRollback.branchName] = True

                    # accounts[writeRollback.accountName] = writeRollback.newBalance
                    # if writeRollback.newBalance < 0:
        self.printBalances(branches)

    def printBalances(self, branches):
        for branchName in branches:
            if branchName == self.participantServer.branchName:
                self.participantServer.printBalances()
            elif branchName in self.branchInfoDict:
                balanceRequest = util.balanceRequest(branchName)
                self.sendToParticipant(branchName, balanceRequest)
        return util.balanceResponse(True)




class ClientThread(Thread):
    def __init__(self, clientsocket, address, participantServer):
        Thread.__init__(self)
        self.clientsocket = clientsocket
        self.address = address
        self.participantServer = participantServer
        self.encoding = participantServer.encoding

    def run(self):
        try:
            query = self.clientsocket.recv(2048).decode(self.encoding)
            queryDict = json.loads(query)
            request = util.toCLS(queryDict)
            response = self.participantServer.processRequest(request)
            # print(f"response from coordinator {response}")
            self.sendResponse(self.clientsocket, response)
        except Exception as err:
            # print("some error occurred in ClientThread")
            # print(err)
            pass

    def sendResponse(self, clientsocket, response):
        jsonData = json.dumps(dataclasses.asdict(response))
        clientsocket.send(jsonData.encode(self.encoding))
        clientsocket.close()

class ParticipantServer():
    def __init__(self, branchName, portNumber, configFile):
        self.branchName = branchName
        self.portNumber = portNumber
        self.configFile = configFile
        self.encoding = "utf-8"
        self.accountsDict = {}
        self.coordinator = Coordinator(self)
        self.startServer()

    def printBalances(self):
        for accountName in self.accountsDict:
            print(f"{accountName} = {self.accountsDict[accountName]}")
        return util.balanceResponse(True)

    def returnBalance(self, request):
        if self.branchName == request.branchName and request.accountName in self.accountsDict:
            return util.accountInfo(self.branchName,request.accountName,self.accountsDict[request.accountName],False, True)
        else:
            return util.accountInfo(self.branchName,request.accountName,-1,False, False) # -1 balance indicates account is not present

    def updateBalance(self, request):
        #update blindly, other checks handled by the coordinator
        self.accountsDict[request.accountName] = request.balance
        #return the updated balance
        return util.accountInfo(request.branchName, request.accountName,request.balance,False,True)

    def processRequest(self, request):
        if request.type == "accountInfo":
            # behaving as a normal participant
            if request.requestType:  # asking for balance
                return self.returnBalance(request)
            else:
                return self.updateBalance(request)
        elif request.type == "balanceRequest":
            return self.printBalances()
        elif request.type == "readRequest":
            return self.coordinator.receivedReadRequest(request)
        elif request.type == "writeRequest":
            return self.coordinator.receivedWriteRequest(request)
        elif request.type == "commitTransaction":
            return self.coordinator.receivedCommitTransaction(request)
        elif request.type == "abortTransaction":
            return self.coordinator.receivedAbortTransaction(request)
        elif request.type == "rollbackUndefinedRead":
            return self.coordinator.receivedRollbackUndefinedRead(request)




    def startServer(self):
        # print("Start Server")
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # bind the socket to a public host, and a well-known port
        serversocket.bind(('', self.portNumber))
        # become a server socket
        # print("listen")
        serversocket.listen(10)
        while True:
            # accept connections from outside
            # print("Accept a connection")
            (clientsocket, address) = serversocket.accept()
            try:
                ClientThread(clientsocket,address,self).start()
            except Exception as err:
                # print('Some error occurred while accepting connections', err)
                pass
        serversocket.close()


if __name__ == "__main__":
    branchName = sys.argv[1]
    configFile = sys.argv[2]
    portNumber = getPortNumber(configFile, branchName)
    if portNumber is None:
        # print("portNumber not found, exiting")
        sys.exit(-1)
    ParticipantServer(branchName,portNumber, configFile)

