from dataclasses import dataclass
import dataclasses
import json, dacite
from dataclasses import field


@dataclass
class accountInfo:
    branchName: str
    accountName: str #A.xyz --> branch name should be included
    balance: int
    requestType: bool #whether it is a request for account info retrieval or update
    success: bool
    noAccount : bool = False
    type: str = "accountInfo"

@dataclass
class readRequest:
    clientId: str
    transactionTime: int
    branchName: str
    accountName: str
    type: str = "readRequest"

@dataclass
class writeRequest:
    clientId: str
    transactionTime: int
    branchName: str
    accountName: str
    balance: int
    type: str = "writeRequest"


@dataclass
class balanceRequest:
    branchName: str
    type: str = "balanceRequest"

@dataclass
class balanceResponse:
    success: bool
    type: str = "balanceResponse"


@dataclass
class commitTransaction:
    clientId: str
    transactionTime: int
    type: str = "commitTransaction"

@dataclass
class abortTransaction:
    clientId: str
    transactionTime: int
    type: str = "abortTransaction"

@dataclass
class commitResponse:
    success: bool
    type: str = "commitResponse"

@dataclass
class abortResponse:
    success: bool
    type: str = "abortResponse"



@dataclass
class writeRollback:
    clientId: str
    transactionTime: int
    branchName: str
    accountName: str
    oldBalance: int
    newBalance: int
    previousWTS : int
    type: str = "writeRollback"

@dataclass
class readRollback:
    clientId: str
    transactionTime: int
    branchName: str
    accountName: str
    previousRTS : int
    type: str = "readRollback"

@dataclass
class rollbackUndefinedRead:
    clientId: str
    transactionTime: int
    branchName: str
    accountName: str
    type: str = "rollbackUndefinedRead"


def toCLS(queryDict):
    if queryDict["type"] == "accountInfo":
        return dacite.from_dict(data_class=accountInfo, data=queryDict)
    elif queryDict["type"] == "readRequest":
        return dacite.from_dict(data_class=readRequest, data=queryDict)
    elif queryDict["type"] == "writeRequest":
        return dacite.from_dict(data_class=writeRequest, data=queryDict)
    elif queryDict["type"] == "commitTransaction":
        return dacite.from_dict(data_class=commitTransaction, data=queryDict)
    elif queryDict["type"] == "abortTransaction":
        return dacite.from_dict(data_class=abortTransaction, data=queryDict)
    elif queryDict["type"] == "commitResponse":
        return dacite.from_dict(data_class=commitResponse, data=queryDict)
    elif queryDict["type"] == "abortResponse":
        return dacite.from_dict(data_class=abortResponse, data=queryDict)
    elif queryDict["type"] == "writeRollback":
        return dacite.from_dict(data_class=writeRollback, data=queryDict)
    elif queryDict["type"] == "rollbackUndefinedRead":
        return dacite.from_dict(data_class=rollbackUndefinedRead, data=queryDict)
    elif queryDict["type"] == "balanceRequest":
        return dacite.from_dict(data_class=balanceRequest, data=queryDict)
    elif queryDict["type"] == "balanceResponse":
        return dacite.from_dict(data_class=balanceResponse, data=queryDict)





