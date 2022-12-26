#!/usr/bin/env python


from queue import PriorityQueue
# python3 mp1_node.py node1 1234 config1.txt
import matplotlib.pyplot as plt
from threading import Thread, Lock, Timer
import collections
import socket
import logging
import sys

nextProposedPriority = 1
nextProposedPriorityMutex = Lock()
totalOrderedQueue = PriorityQueue()
totalOrderedQueueMutex = Lock()
accounts = {}
deliveredMsgs = {}
finalPriority = {}
proposedPriority = {}
proposedPriorityMutex = Lock()
finalPriorityMutex = Lock()
accountsMutex = Lock()
myNodeId=None
groupMembers = []
numberOfMembers=None
mutex = Lock()
length = 0
lengthList=[]

TIME_SAMPLED = 103 #


def displayBandwidth():
    plt.clf()
    X = [i for i in range(len(lengthList))]
    plt.ylabel("message length(# of chars)")
    plt.xlabel("relative time (1 sec window)")
    plt.plot(X, lengthList, label="bandwidth")
    plt.legend()
    plt.savefig('{}_bandwidth.png'.format(myNodeId))


def processData(wordLength):
    mutex.acquire()
    try:
        global length
        length += wordLength
    finally:
        mutex.release()

def computeStats():
    mutex.acquire()
    try:
        global length
        lengthList.append(length)
        length = 0
        if len(lengthList) == TIME_SAMPLED:
            displayBandwidth()
            sys.exit(0)
    finally:
        mutex.release()


def deliveryCheckInLoop():
    deliverable = checkForDelivery()
    while deliverable != None:
        if (deliverMsg(deliverable)):
            print(deliverable.msg  + "SUCCESSFUL", deliverable.priority, deliverable.nodeId)
            printAccountBalances()
        else:
            print(deliverable.msg + " FAILED")
            printAccountBalances()
        logging.info(deliverable.msg)
        deliverable = checkForDelivery()

class StringIterator():
    def __init__(self):
        self.line = ''
        self.complete = 0
        self.flag = 0
        self.nodeName = ''

    def getNodeName(self):
        return self.nodeName
    def getLine(self, message):
        words = None
        for c in message:
            if c == '$':
                self.flag = self.flag + 1
                if self.flag == 2:
                    self.flag = 0
                    words = self.line
                    self.line = ""
            else:
                self.line = self.line + c
        return words

def execute(transaction):
    global numberOfMembers
    proposedPriority = getNextProposedPriotity()
    print("In Execute, Transaction is: ", transaction,"Priority from client that received it: " , proposedPriority)
    msg = Message(proposedPriority, myNodeId, False, transaction)
    addMessage(msg, True)
    # create a duplicate msg, 
    msg = Message(msg.priority, msg.nodeId, msg.final, msg.msg)
    print(msg.msg)

    count = getPrioritesFromAllMembers(msg)
    secondTime  = updateMessageFinalPriority(msg)
    if not secondTime:
        deliveryCheckInLoop()
    # if count < numberOfMembers/2:
    #     print("Only " + count + " out of " + numberOfMembers + " nodes has responded for PROPOSED PRIORITY MESSAGE !! so failing this transaction")
    #     return # TODO need to remove the transaction from my as well as other's priority queue
    count = multicastFinalPriority(msg)
    # if count < numberOfMembers/2:
    #     print("Only " + count + " out of " + numberOfMembers + " nodes has responded for OBSERVED PRIORITY MESSAGE !! so failing this transaction")
    #     return # TODO need to remove the transaction from my as well as other priority queue


def extractNos(word):
    number = ''
    for c in word:
        if c >= '0' and c <= '9':
            number += c
    return int(number)

def configParser(fileName, nodeId):
    global numberOfMembers
    global groupMembers
    global myNodeId
    myNodeId = int(nodeId)
    file = open(fileName, 'r')
    lines = file.readlines()
    count = 0
    for line in lines:
        count += 1
        if count == 1:
            numberOfMembers = int(line)
        else:
            print("second line",line)
            words = line.split(" ")
            print(words)
            print(socket.gethostname() ,  words[1])
            if socket.gethostname() in words[1]:
                continue
            groupMembers.append((extractNos(words[0]), words[1], words[2]))
    logging.basicConfig(filename='node{}.log'.format(myNodeId), level=logging.INFO, filemode='w', format='%(asctime)s:%(message)s')
    file.close()

class Message():
    def __init__(self, priority, nodeId, final, msg):
        self.priority = int(priority)
        self.nodeId = int(nodeId)
        self.final = final
        self.msg = msg
        return

def addMessage(msg, proposed):
    global proposedPriority
    check = True
    if proposed and msg.msg not in proposedPriority:
        proposedPriorityMutex.acquire()
        proposedPriority[msg.msg] = msg.priority, msg.nodeId
        proposedPriorityMutex.release()
    elif msg.msg in proposedPriority:
        proposedPriorityMutex.acquire()
        p , n = proposedPriority[msg.msg]
        if p == msg.priority and n == msg.nodeId:
            check = False
        proposedPriorityMutex.release()
    if not check:
        return
    global totalOrderedQueue
    totalOrderedQueueMutex.acquire()
    try:
        totalOrderedQueue.put((msg.priority, msg.nodeId, msg))
        print(totalOrderedQueue)
    finally:
        totalOrderedQueueMutex.release()


def printAccountBalances():
    global accounts
    oderedAccounts = collections.OrderedDict(sorted(accounts.items()))
    print("BALANCES"),
    for k, v in oderedAccounts.items():
        print(" ", k, ":", v),


def deliverMsg(msg):
    global deliveredMsgs
    updated = msg.msg.split("!")[1]
    print("UPDATED", updated)
    words = updated.split(" ")
    print("WORDS", words)
    success = False
    global accounts
    try:
        accountsMutex.acquire()
        if words[0] == "DEPOSIT":
            success = True
            if words[1] not in accounts:
                accounts[words[1]] = int(words[2])
            else:
                accounts[words[1]] = accounts[words[1]] + int(words[2])
        elif words[1] in accounts and int(words[-1]) <= accounts[words[1]]:
            success = True
            accounts[words[1]] = accounts[words[1]] - int(words[-1])
            if words[-2] not in accounts:
                accounts[words[-2]] = int(words[-1])
            else:
                accounts[words[-2]] = accounts[words[-2]] + int(words[-1])
    finally:
        accountsMutex.release()
    if success:
        deliveredMsgs[msg] = True
    return success


# should be call in a loop whenever you get a meesage
def checkForDelivery():
    global totalOrderedQueue
    totalOrderedQueueMutex.acquire()
    finalMsg = None
    try:
        condition = True
        while condition and not(totalOrderedQueue.empty()):
            # print("insied check for deliveryLoop" , len(totalOrderedQueue) , totalOrderedQueue)
            _, _, obj = totalOrderedQueue.queue[0]
            if obj.msg in deliveredMsgs:
                totalOrderedQueue.get()
            elif obj.msg in finalPriority:
                dict = finalPriority[obj.msg]
                _, _, tmp = totalOrderedQueue.get()
                if obj.priority == dict["priority"] and obj.nodeId == dict["nodeId"]:
                    finalMsg = tmp
                    condition = False # break now got the message
            else:
                condition = False
    finally:
        totalOrderedQueueMutex.release()
    return finalMsg



def updateMessageFinalPriority(msg):
    finalPriorityMutex.acquire()
    global finalPriority
    secondTime = False
    try:
        if msg.msg not in finalPriority:
            finalPriority[msg.msg] = {}
            dict = finalPriority[msg.msg]
            dict["priority"] = msg.priority
            dict["nodeId"] = msg.nodeId
        else:
            secondTime = True
    finally:
        finalPriorityMutex.release()
    if not secondTime:
        addMessage(msg, False)
    return secondTime


def getNextProposedPriotity():
    nextProposedPriorityMutex.acquire()
    try:
        global nextProposedPriority
        val = nextProposedPriority
        nextProposedPriority += 1
    finally:
        nextProposedPriorityMutex.release()
    return val

def handleFailure(NODE_NAME, index):
    ##remove node from memebers and other DS's
    global totalOrderedQueue
    global groupMembers
    totalOrderedQueueMutex.acquire()
    try:
        temp = PriorityQueue()

        for priorityTuple in totalOrderedQueue.queue:
            priority, nodeId, msg = priorityTuple
            if nodeId!=NODE_NAME:
                temp.put(priorityTuple)
        groupMembers.pop(index)
        totalOrderedQueue=temp
    finally:
        totalOrderedQueueMutex.release()

def unicastSend(msg, index, proposedFlag, initStep = False):
    global groupMembers
    global myNodeId
    priority = None
    try:

        NODE_NAME = None
        print("The proposed flag is ", proposedFlag)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(10)
            NODE_NAME, HOST, PORT = groupMembers[index]
            PORT = int(PORT)
            NODE_NAME = NODE_NAME
            print("THIS IS NODE NAME", NODE_NAME)
            print("The proposed flag is ", proposedFlag)
            if proposedFlag:
                #asking for prioirities priority
                content = "$* " + msg.msg + "$"
            else:
                # reply with your prioirty
                print("After " + "$" + str(msg.msg) + "%" + str(msg.priority) + "%" + str(msg.nodeId) + "$")
                content = "$" + str(msg.msg) + "%" + str(msg.priority) + "%" + str(msg.nodeId) + "$"

            print("The content send to server is " + content)
            s.connect((HOST, PORT))
            s.send(content.encode())
            processData(len(content))

            priority = s.recv(2048).decode("utf-8")#TODO : CHECKOUTSOMETHING, WHAT IF SOCKET IS FULL OF DATA, DID WE JUST READ THE PRIORIRITY OR MORE?
            s.close()
            processData(sys.getsizeof(priority))
            print('Content got from Server ', priority)
    except socket.timeout as err:
        print('There is a timeout in Client !! ', initStep, groupMembers[index])
        print(err)
        print('Client message is ' + msg.msg , msg.priority , msg.nodeId , proposedFlag, priority, myNodeId)
        if not initStep and priority is None:
            handleFailure(NODE_NAME, index)
    finally:
        s.close()
        return priority, NODE_NAME


def getPrioritesFromAllMembers(msg):
    global numberOfMembers
    count = 0  # sanity check if more members failed
    # todo - send request parallely
    for x in range(numberOfMembers-1):
        print("Number Of Members " , numberOfMembers , " current member " , x , " size of group member is " , len(groupMembers))
        p, id = unicastSend(msg, x, True)
        p = int(p)
        id = int(id)
        print("the response is ", p , id)
        #when?
        if p == None or id == None:
            continue
        count = count + 1
        if msg.priority <= p:
            msg.priority = p
            if msg.nodeId <= id:
                msg.nodeId = id
    return count


def init():
    global numberOfMembers
    count = 0
    for x in range(numberOfMembers - 1):
        msg = Message(0, myNodeId, False, "INIT_MESSAGE")
        p, id = unicastSend(msg, x, False, initStep=True)
        if p == None:
            return count
        count = count + 1
    return count == numberOfMembers-1


def multicastFinalPriority(msg):
    global numberOfMembers
    count = 0
    print("multicast final priority ", msg, " msg is ", numberOfMembers)
    for x in range(numberOfMembers - 1):
        p, id = unicastSend(msg, x, False)
        if p == None:
            continue
        count = count + 1
    return count

#create a thread that wakes up every second and calls compute stats.
def do_every(interval, worker_func, iterations=0):
    if iterations != 1:
        Timer(
            interval,
            do_every, [interval, worker_func, 0 if iterations == 0 else iterations-1]
        ).start()
        worker_func()

#delte it, but notimmediatley 

#maximum 1-way delay.eventually. wait amount till final . r0multicast. . 

#tHOUGHTS:

#detect failures in two intances, one, when we ask the process about the priority and we await a reply. Two way delay should suffice
#the other is if they asked us for a priority, but never sent us back the final priority. in that case it is 