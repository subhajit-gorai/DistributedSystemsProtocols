#!/usr/bin/env python
import collections
import socket
import sys
import threading
import time
from util import *
import matplotlib.pyplot as plt

myNodeId=None

def serverResponse(newPriority, clientsocket, address):
    strPriorirty = str(newPriority)
    clientsocket.send(strPriorirty.encode())
    processData(len(strPriorirty))
    clientsocket.close()


class ClientThread(Thread):

    def __init__(self, clientsocket, address):
        Thread.__init__(self)
        self.clientsocket = clientsocket
        self.address = address
        self.encoding = 'utf-8'
        self.stringIterator = StringIterator()

    def run(self): #TODO add stopping conditions - when msgs are fetched in parts
        global myNodeId
        while True:
            # print("client thread in a loop")
            secondTime = False
            msg = self.clientsocket.recv(2048).decode(self.encoding)
            words = self.stringIterator.getLine(message=msg)
            print('words are ', words, " and msg is " + msg)
            processData(len(msg))
            if 'INIT_MESSAGE' in words:
                serverResponse(0,self.clientsocket, self.address)
                return

            #asking for final priority
            elif words[0] == "*":
                newPriority = getNextProposedPriotity()
                msg = Message(newPriority, myNodeId, False, "".join(words[1:]).lstrip().rstrip())
                addMessage(msg, True)
                serverResponse(newPriority, self.clientsocket, self.address)

            #telling me final priority
            else:
                arr = words.split("%")
                print("getting final priority for msg " + arr[0].lstrip().rstrip() + " the priority is " + arr[1] + "." + arr[2])
                msg = Message(int(arr[1]), int(arr[2]), True, arr[0].lstrip().rstrip())
                secondTime = updateMessageFinalPriority(msg)
                serverResponse(-1, self.clientsocket, self.address)
                if not secondTime:
                    multicastFinalPriority(msg)
            if not secondTime:
                deliveryCheckInLoop()
            return


def runServer():
    print("Start Server")
    if len(sys.argv) <= 1:
        # print >> sys.stderr, "Port No. not given to, please provide port no. Server start failed"
        sys.exit(1)
    PORT = int(sys.argv[2])
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # bind the socket to a public host, and a well-known port
    serversocket.bind(('', PORT))
    # become a server socket
    print("listen")
    serversocket.listen(10)
    threads = []

    while True:
        # accept connections from outside
        print("Accept a connection")
        (clientsocket, address) = serversocket.accept()
        try:
            newthread = ClientThread(clientsocket, address)
            newthread.start()
            threads.append(newthread)

        except:
            print('Some error occurred')
    for t in threads:
        t.join()
    serversocket.close()

def main():
    print("thread 1")
    i=0
    while True:
        if init():
            break
        else:
            time.sleep(2)
    print('successful init ')
    do_every (1, computeStats)
    while True:
        print("asking for input")
        line = input()
        execute(str(i)+"n"+str(myNodeId)+"!"+line)
        processData(len(line))
        i+=1



if __name__ == "__main__":
    # global myNodeId
    print("Start")
    myNodeId = extractNos(sys.argv[1])
    print('My node id is ', myNodeId)
    PORT = int(sys.argv[2])
    configFile = sys.argv[3]
    configParser(configFile, myNodeId)
    #time.sleep(15)
    thread1 = threading.Thread(target=main)
    thread2 = threading.Thread(target=runServer)
    thread1.daemon = True
    thread2.daemon = True
    thread2.start()
    thread1.start()
    thread1.join()
    thread2.join()

# DEPOSIT i 95
# DEPOSIT e 36
# DEPOSIT f 45
# TRANSFER e -> l 19
# TRANSFER l -> k 5
# DEPOSIT t 75
# DEPOSIT w 6
# TRANSFER l -> c 9
# TRANSFER f -> m 8
# TRANSFER w -> s 4
# TRANSFER l -> n 3
# TRANSFER w -> z 1
# TRANSFER l -> n 2
# TRANSFER t -> g 14

# cd .\MP1\DistributedSystems\MP2\

# python3 mp1_node.py node1 1234 config1.txt
# python3 mp1_node.py node2 1235 config2.txt