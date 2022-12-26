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


class Client():
    def __init__(self, clientId,configFile):
        self.clientId = clientId
        self.encoding = "utf-8"
        self.setCoordinatorCreds(configFile)
        if self.coordinatorHost == None:
            # print("coordinator not set")
            sys.exit(-1)
        self.readCommands()

    def setCoordinatorCreds(self,configFile):
        self.configFile = configFile
        #choose the lexicographically smallest branch as the Coordinator
        self.coordinatorName = "Z"
        self.coordinatorHost = None
        self.coordinatorPort = None
        self.branchInfoDict = {} #{A: {host:abc.xyz, port:1234}, B :{host:cdf.weq, port: 1245}}
        file = open(self.configFile, 'r')
        lines = file.readlines()
        for line in lines:
            words = line.split(" ")
            if len(words) == 3:
                self.branchInfoDict[words[0]] = {"host":words[1], "port":int(words[2])}
            # TODO check if this comparison is valid in python or not
            if words[0] <= self.coordinatorName:
                self.coordinatorName = words[0]
                self.coordinatorHost = words[1]
                self.coordinatorPort = int(words[2])
        file.close()

    def readCommands(self):
        self.transactionStart = False
        while True:
            line = input()
            line = line.strip()
            if line == "BEGIN":
                if self.transactionStart:
                    # print("previous transaction not closed but started a new one !! ", file=sys.stderr)
                    continue
                print("OK")
                self.transactionTime = time.time_ns()
                # self.transactionTime = int(time.time())
                self.transactionStart = True
            elif line == "COMMIT":
                if not self.transactionStart:
                    # print("transaction not yet started but trying to commit !!", file=sys.stderr)
                    continue
                self.commitTransaction()
                self.transactionTime = None
                self.transactionStart = False
            elif self.transactionStart: #it is a command, so transaction must have been started
                words = line.split(" ")
                if words[0] == "DEPOSIT":
                    self.runDepositCommand(words[1], int(words[2]))
                elif words[0] == "WITHDRAW":
                    self.runWithdrawCommand(words[1], int(words[2]))
                elif words[0] == "BALANCE":
                    self.runBalanceCommand(words[1])
                elif words[0] == "ABORT":
                    self.runAbortCommand()
                    self.transactionTime = None
                    self.transactionStart = False


    def sendCommandsToCoordinator(self, command):
        requestData = json.dumps(dataclasses.asdict(command))
        # print(f"requested data is {requestData}")
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # TODO set appropriate timeout
                # s.settimeout(10)
                # print("coordinator Host " , self.coordinatorHost)
                # print("coordinator Port " , self.coordinatorPort)
                #TODO add coordinatorHost
                s.connect((self.coordinatorHost, self.coordinatorPort))
                s.send(requestData.encode(self.encoding))
                response = s.recv(2048).decode(self.encoding)
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

    def commitTransaction(self):
        #you have transaction time and client id .. to uniquely identify a transaction
        commitRequest = util.commitTransaction(self.clientId, self.transactionTime)
        commitResponse = self.sendCommandsToCoordinator(commitRequest)
        if commitResponse.success:
            print("COMMIT OK")
        else:
            print("ABORTED")


    def runDepositCommand(self, accountName, amount):
        readRequest = util.readRequest(self.clientId, self.transactionTime, accountName.split(".")[0],accountName)
        # print(f"read Request is {readRequest}, and now sending to coordinator")
        accountInfo = self.sendCommandsToCoordinator(readRequest)
        readAmount = accountInfo.balance
        writeRequest = util.writeRequest(self.clientId,self.transactionTime,accountName.split(".")[0], accountName,readAmount+amount)
        accountInfo = self.sendCommandsToCoordinator(writeRequest)
        # DEPOSIT is always successfull, let coordinator handle the discrepancies
        print("OK")

    def runWithdrawCommand(self, accountName, amount):
        readRequest = util.readRequest(self.clientId, self.transactionTime, accountName.split(".")[0], accountName)
        accountInfo = self.sendCommandsToCoordinator(readRequest)
        if accountInfo.success and not accountInfo.noAccount:
            readAmount = accountInfo.balance
            writeRequest = util.writeRequest(self.clientId, self.transactionTime, accountName.split(".")[0], accountName,readAmount-amount)
            modifiedAccountInfo = self.sendCommandsToCoordinator(writeRequest)
            if modifiedAccountInfo.success:
                print("OK")
            else: #coordinator needs to handle the consistencty check while committing transactions
                print("OK")
        else:
            print("NOT FOUND, ABORTED")
            self.transactionStart = False

    def runBalanceCommand(self, accountName):
        readRequest = util.readRequest(self.clientId, self.transactionTime, accountName.split(".")[0], accountName)
        accountInfo = self.sendCommandsToCoordinator(readRequest)
        if accountInfo.success and not accountInfo.noAccount:
            print(f"{accountInfo.accountName} = {accountInfo.balance}")
        else:
            print("NOT FOUND, ABORTED")
            rollbackUndefinedRead = util.rollbackUndefinedRead(self.clientId, self.transactionTime, accountName.split(".")[0], accountName)
            self.sendCommandsToCoordinator(rollbackUndefinedRead)
            self.transactionStart = False
            self.transactionTime = None

    def runAbortCommand(self):
        abortRequest = util.abortTransaction(self.clientId,self.transactionTime)
        abortResponse = self.sendCommandsToCoordinator(abortRequest)
        if abortResponse.success:
            print("ABORTED")
        else:
            # print("error while aborting")
            pass





if __name__ == "__main__":

    clientId = sys.argv[1]
    configFile = sys.argv[2]
    Client(clientId,configFile)