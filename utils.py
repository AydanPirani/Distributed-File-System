import re

# INTRODUCER_HOST = "fa22-cs425-3610.cs.illinois.edu"
INTRODUCER_HOST = "fa22-cs425-3601.cs.illinois.edu"

# map < str: map <int: list> >
fileStructure = None

class Status:
    NEW = 'NEW'
    RUNNING = 'RUNNING'
    LEAVE = 'LEAVE'

class Type:
    PING = "Ping"
    PONG = "Pong"
    JOIN = "Join"
    SEND = "Send"
    FILES = "Files"

# TODO: Change membership list SEND to be more descriptive
class SDFS_Type:
    PUT = "Put"
    GET = "Get"
    SEND_FILE = "Send_File"
    RECEIVE_FILE = "Receive_File"
    FILES = "Files"
    ROUTE_PUT = "Route_Put"
    ROUTE_GET = "Route_Get"

class Field:
    TYPE = "Type"
    MEMBERSHIP = "Membership"

def get_neighbors(host):
    print(host)
    number = int(re.findall(r'36(.+).c', host)[0])
    predecessor = number - 1
    successor = number + 1
    for i in range(3):
        if i < 2:
            if predecessor < 1:
                yield "fa22-cs425-36%02d.cs.illinois.edu" % (10 + predecessor)
            else:
                yield "fa22-cs425-36%02d.cs.illinois.edu" % predecessor
            predecessor -= 1
        else:
            if successor > 10:
                yield "fa22-cs425-36%02d.cs.illinois.edu" % (successor - 10)
            else:
                yield "fa22-cs425-36%02d.cs.illinois.edu" % successor
            successor += 1


def get_all_hosts():
    l = []
    for i in range(1, 11):
        l.append("fa22-cs425-36%02d.cs.illinois.edu" % i)
    return l

def elem(s):
    return next(iter(s))
    