import datetime
import time

import os
import shutil

import json
import logging

import socket
import threading
import utils

import random

from logging.handlers import RotatingFileHandler

import mp1_client
import mp1_server

HOST = socket.gethostname()
IP = socket.gethostbyname(HOST)
PORT = 23333
SIZE = 4096

# So you send pings so that the nodes that you've pinged update their memebership list at the value of the pinging node 
# and so that they update their membership lists with all the extra values of the pinging nodes membership list
# When you send a pong, the node that receives pong updates its membership list at the value of the sending pong's node

# define file logging info
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    filename=  'host.log',
                    filemode='w')
# define a handler that displays ERROR messages to the terminal
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
rotating_file_handler = RotatingFileHandler('host.log', maxBytes=102400000, backupCount=1)
logging.getLogger('').addHandler(rotating_file_handler)
recv_logger = logging.getLogger('receiver')
monitor_logger = logging.getLogger('monitor')
join_logger = logging.getLogger('join')
send_logger = logging.getLogger('send')


class Server:
    def __init__(self):
        timestamp = str(int(time.time()))
        # membership list, key: host, value: (timestamp, status)
        self.MembershipList = {
            HOST: (timestamp, utils.Status.LEAVE)}
        self.time_lock = threading.Lock()
        self.ml_lock = threading.Lock()
        # record the time current process receives last ack from its neighbors
        self.last_update = {}
        self.fileStructure = dict()
        self.INTRODUCER_HOST = utils.INTRODUCER_HOST

        self.send_lock = threading.Lock()
        self.recv_lock = threading.Lock()


    def join(self):
        '''
        Contacts the introducer that the process will join the group and uptate its status.

        return: None
        '''
        print("start joining")
        timestamp = str(int(time.time()))
        join_logger.info("Encounter join before:")
        join_logger.info(self.MembershipList)

        # Clear the files directory upon joining, and re-generate the directory
        shutil.rmtree(".files", ignore_errors=True)
        os.mkdir(".files")

        # change the status to running when it sends a message to the introducer or when it is introducer
        self.MembershipList[HOST] = (timestamp, utils.Status.RUNNING)
        join_logger.info("Encounter after before:")
        join_logger.info(self.MembershipList)
        outgoing_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # print(f"in join! host={HOST}, introducer={utils.INTRODUCER_HOST}")
        if HOST != self.INTRODUCER_HOST:
            # send a message that this node wants to join to the introducer
            join_msg = [utils.Type.JOIN, HOST, self.MembershipList[HOST]]
            outgoing_socket.sendto(json.dumps(join_msg).encode(), (self.INTRODUCER_HOST, PORT))
        else:
            print("This is introducer host!")

            for h in self.MembershipList:
                join_msg = [utils.Type.FILES, HOST, self.fileStructure]
                outgoing_socket.sendto(json.dumps(join_msg).encode(), (h, PORT))


    def send_ping(self, host):
        '''
        Send PING to current process's neighbor using UDP. If the host is leaved/failed, then do nothing.

        return: None
        '''
        print("sender started")
        outgoing_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while True:
            time.sleep(0.3)
            # if the host to send to is not in the MembershipList/leaving or if the current HOST is not leaving, don't ping
            if self.MembershipList[HOST][1] == utils.Status.LEAVE or host not in self.MembershipList or self.MembershipList[host][1] == utils.Status.LEAVE:
                continue
            try:
                self.ml_lock.acquire()
                # get curr time
                timestamp = str(int(time.time()))
                send_logger.info("Encounter send before:")
                send_logger.info(self.MembershipList)
                # update curr HOST entry in membership list with timestamp
                self.MembershipList[HOST] = (timestamp, utils.Status.RUNNING)
                send_logger.info("Encounter send after:")
                send_logger.info(self.MembershipList)
                
                # send ping with curr HOST, MembershipList
                ping_msg = [utils.Type.PING, HOST, self.MembershipList]
                outgoing_socket.sendto(json.dumps(ping_msg).encode(), (host, PORT))
                # update the last updated time of the last host the info was sent to 
                if host in self.MembershipList and host not in self.last_update:
                    self.time_lock.acquire()
                    # update the time that the host received its last ack from it's neighbor, HOST
                    self.last_update[host] = time.time()
                    self.time_lock.release()
                self.ml_lock.release()
            except Exception as e:
                print(e)


    def sdfs_program(self):
        print("sdfs receiver started")

        sdfs_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sdfs_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        print(PORT)
        sdfs_socket.bind((HOST, PORT + 1))

        while True:
            # print("in recv!")
            try:
                data, addr = sdfs_socket.recvfrom(SIZE)
                if data:
                    arg, target, local_filename, sdfs_filename = json.loads(data.decode())
                    if arg == utils.SDFS_Type.ROUTE_PUT:
                        self.route_put(local_filename, sdfs_filename, target)
                    if arg == utils.SDFS_Type.RECEIVE_FILE:
                        thread = threading.Thread(target = self.receive_file, args = (sdfs_filename, (target, PORT + 2)))
                        thread.start()
            except:
                pass


    def detector_program(self):
        '''
        Handles receives in different situations: PING, PONG and JOIN
        When reveived PING: update membership list and send PONG back to the sender_host
        When received PONG: delete the sender_host from last_update table and update membership list
        When received JOIN: update the membership list and notify other hosts if you are the introducer host
        
        return: None
        '''
        print("detector receiver started")
        detection_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        detection_socket.bind((HOST, PORT))

        

        recv_logger.info('receiver program started')
        while True:
            try:
                # if LEAVE status, don't do anything
                if self.MembershipList[HOST][1] == utils.Status.LEAVE:
                    recv_logger.info("skip receiver program since " + HOST + " is leaved")
                    continue
                data, addr = detection_socket.recvfrom(SIZE)
                recv_logger.info("connection from: " + str(addr) + " with data: " + data.decode())
                if data:
                    request = data.decode()
                    request_list = json.loads(request)
                    sender_host = request_list[1]
                    request_type = request_list[0]
                    
                    request_membership = request_list[2]
            
                    self.ml_lock.acquire()
                    # everytime a ping with JOIN status is reveived, set that node to NEW in the current HOSTS's membership lisy
                    # if the current node, HOST, is an introducer, send the JOIN message out to all of the nodes
                    if request_type == utils.Type.JOIN:
                        recv_logger.info("Encounter join before:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        # if a ping is received with the JOIN status, set status to NEW 
                        self.MembershipList[sender_host] = (str(int(time.time())), utils.Status.NEW)
                        recv_logger.info("Encounter join after:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        
                        if HOST == self.INTRODUCER_HOST:
                            recv_logger.info("introducer recv connection from new joiner: " + str(addr))
                            
                            join_msg = [utils.Type.JOIN, sender_host, self.MembershipList[sender_host]]
                            hosts = utils.get_all_hosts()
                            ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            for hostname in hosts:
                                if hostname != HOST and hostname != sender_host:
                                    ss.sendto(json.dumps(join_msg).encode(), (hostname, PORT))

                    elif request_type == utils.Type.PING:
                        recv_logger.info("Encounter PING before:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        # for all items in the membership_list of the node that sent the request,
                        for host, value in request_membership.items():
                            timestamp, status = value[0], value[1]
                            # if the status is LEAVE, update that host's value in your current host's membership list
                            if status == utils.Status.LEAVE:
                                self.MembershipList[host] = value

                            # if the current host is not in the membershiplist, add it 
                            if host not in self.MembershipList:
                                self.MembershipList[host] = value
                                continue
                        
                            # if the request node's timestamp is greater than the current node's timestamp, 
                            # then update the timestamp and status
                            if int(timestamp) > int(self.MembershipList[host][0]):
                                self.MembershipList[host] = (timestamp, status)
                        recv_logger.info("Encounter PING after:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        # send a pong back to the request node
                        pong = [utils.Type.PONG, HOST, self.MembershipList[HOST]]
                        
                        detection_socket.sendto(json.dumps(pong).encode(), (sender_host, PORT))

                    elif request_type == utils.Type.PONG:
                        recv_logger.info("Encounter PONG before:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        # update membership_list entry of the current HOST at the request node's entry
                        self.MembershipList[sender_host] = request_membership
                        # remove it from the last_update list.
                        # Because the ack has been processed and the pong has been received
                        if sender_host in self.last_update:
                            self.time_lock.acquire()
                            self.last_update.pop(sender_host, None)
                            self.time_lock.release()
                        recv_logger.info("Encounter PONG after:")
                        recv_logger.info(json.dumps(self.MembershipList))
                    elif request_type == utils.Type.SEND:
                        # retrieve file and send to sender_host(newReplicaNodeHost)
                        pass
                    elif request_type == utils.Type.FILES:
                        # retrieve file and send to sender_host(newReplicaNodeHost)
                        self.fileStructure = request_membership
                        
                    else:
                        recv_logger.error("Unknown message type")
                    self.ml_lock.release()
            except Exception as e:
                print(e)


    def assignLeader(self):
        maximum = 0
        for node in self.MembershipList:
            # check new vs running? 
            if (self.MembershipList[node][1] != utils.Status.LEAVE and node > maximum):
                maximum = node
                
        self.INTRODUCER_HOST = maximum
        

    def monitor_program(self):
        '''
        Monitor daemon that checks if any neighbor process has timeout

        return: None
        '''
        print("monitor started")
        detection_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while True:
            try:
                self.time_lock.acquire()
                
                # loop through all of the nodes that are still waiting for a pong back
                # if there's a timeout, then set the status to LEAVE
                keys = list(self.last_update.keys())
                for hostname in keys:
                    if time.time() - self.last_update[hostname] > 2:
                        value = self.MembershipList.get(hostname, "*")
                        if value != "*" and value[1] != utils.Status.LEAVE:
                            monitor_logger.info("Encounter timeout before:")
                            monitor_logger.info(json.dumps(self.MembershipList))
                            self.MembershipList[hostname] = (value[0], utils.Status.LEAVE)
                            #if intro 

                            if(HOST == self.INTRODUCER_HOST):
                                # fix -> check if the newNode is status leave, if it is, choose new random node
                                newReplicaNodeHost, newReplicaNodeValue = random.choice(list(self.MembershipList.values()))
                                # newReplicaNode = random.randint(0, len(self.MembershipList))
                                while (newReplicaNodeValue[1] != utils.Status.LEAVE ):
                                    newReplicaNodeHost, newReplicaNodeValue = random.choice(list(self.MembershipList.values()))
                                for fileName in fileStructure.keys():
                                    for version in fileName.keys():
                                        if hostname in fileName[version]:
                                            fileName[version].remove(hostname)
                                            #pick another node in this list that has the versioned file 
                                            fileContainingReplica = fileName[version][0]
                                            # send a message to fileContainingReplica telling it to send its versioned file to newReplicaNode
                                            join_msg = [utils.Type.SEND, newReplicaNodeHost, version]
                                            # TODO: Ensure that we have another socket to listen
                                            # print("sending", join_msg)
                                            detection_socket.sendto(json.dumps(join_msg).encode(), (fileContainingReplica, PORT+1))
                                            # add a new request_type for this^?
                            
                            monitor_logger.info("Encounter timeout after:")
                            monitor_logger.info(json.dumps(self.MembershipList))
                        self.last_update.pop(hostname, None)
                
                self.time_lock.release()
            except Exception as e:
                print(e)

    # simply updates the value at the current host to a LEAVE status
    def leave(self):
        '''
        Mark current process as LEAVE status

        return: None
        '''
        self.time_lock.acquire()
        prev_timestamp = self.MembershipList[HOST][0]
        monitor_logger.info("Encounter leave before:")
        monitor_logger.info(json.dumps(self.MembershipList))
        self.MembershipList[HOST] = (prev_timestamp, utils.Status.LEAVE)
        monitor_logger.info("Encounter leave after:")
        monitor_logger.info(json.dumps(self.MembershipList))
        print(self.MembershipList)
        self.time_lock.release()


    def print_membership_list(self):
        '''
        Print current membership list
        
        return: None
        '''
        
        print(self.MembershipList)
       

    def print_self_id(self):
        '''
        Print self's id
        
        return: None
        '''
       
        print(IP + "#" + self.MembershipList[HOST][0])
        
    # This will run on a thread, IF the receiver gets a SEND/GET request
    # Note that addr is the address of PORT + 2
    def receive_file(self, filename, addr):
        self.recv_lock.acquire()
        recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        recv_socket.bind((HOST, PORT + 3))

        # Send ACK to sender, indicating ready to download
        recv_socket.sendto("Ready".encode(), addr)

        # Receive the size from the sender
        data, _ = recv_socket.recvfrom(SIZE)
        size = int(data.decode())
        recv_socket.sendto("Size Received".encode(), addr)
        
        with open(f".files/{filename}", "w+") as f:
            while size > 0: 
                data, _ = recv_socket.recvfrom(SIZE)
                f.write(data.decode())
                size -= len(data)
        recv_socket.sendto("File received".encode(), addr)
        recv_socket.close()
        self.recv_lock.release()
        return

    # This will also run on a thread, hosted on PORT + 2
    # Assumptions: local filename does exist, also that this is routed through the leader
    def send_file(self, local_filename, sdfs_filename, target):
        self.send_lock.acquire()
        send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        send_socket.bind((HOST, PORT + 2))
        recv_addr = (target, PORT + 3)

        # Send SDFS_filename to receiver, wait for ACK
        query = [utils.SDFS_Type.RECEIVE_FILE, HOST, local_filename, sdfs_filename]
        send_socket.sendto(json.dumps(query).encode(), (target, PORT + 1))
        data, _ = send_socket.recvfrom(SIZE)
        msg = data.decode()

        # Send the size to the target, wait for ACK
        size = os.path.getsize(local_filename)
        send_socket.sendto(str(size).encode(), recv_addr)
        data, _ = send_socket.recvfrom(SIZE)
        msg = data.decode()
        print(msg)
        
        # Send the data to the target, wait for ACK
        with open(local_filename, "r") as f:
            send_socket.sendto(f.read().encode(), recv_addr)
        data, _ = send_socket.recvfrom(SIZE)
        msg = data.decode()

        send_socket.close()
        self.send_lock.release()
        return
        

    # If you receive a route_put: you need to send a file to the given target
    def route_put(self, local_filename, sdfs_filename, target):
        t = threading.Thread(target = self.send_file, args = (local_filename, sdfs_filename, target))
        t.start()

    def put(self, local_filename, sdfs_filename, target):
        sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if HOST != self.INTRODUCER_HOST:
            # Reroute the put request to the leader
            query = [utils.SDFS_Type.PUT, target, local_filename, sdfs_filename]
            sender_socket.send(json.dumps(query).encode(), (self.INTRODUCER_HOST, PORT + 1))
        else:
            N = len(self.MembershipList)
            
            # Generate the next node that isn't dead, given a certain offset from the memebrship list
            def get_next_alive_node(start):
                keys = list(self.MembershipList.keys())

                for i in range(len(keys)):
                    idx = (i + start) % len(keys)
                    key = keys[idx]
                    if self.MembershipList[key][1] != utils.Status.LEAVE:
                        return key

            # Randomized to evenly distribute the node progress
            n1 = get_next_alive_node(random.randrange(N))
            n2 = get_next_alive_node(random.randrange(N))
            n3 = get_next_alive_node(random.randrange(N))

            replica_set = set([n1, n2, n3])
            print(f"replica set = {replica_set}")

            # TODO: Given a replica set, route protocol instructions to sender, and work from there
            for i in replica_set:
                q = [utils.SDFS_Type.ROUTE_PUT, i, local_filename, sdfs_filename]
                print(f"sending query {q} to {i}:{PORT + 1}")
                # Send a route back to the sender, telling it to send the file to the given nodes
                sender_socket.sendto(json.dumps(q).encode(), (target, PORT + 1))
            # TODO: Update local file directory structure
            

    def shell(self):
        print("""Please use the following codes for the below functionalities:\n
                \r\t1. put: add a file to the file system
                \r\t2. get: get a file from the file system
                \r\t3. delete: delete a file from the file system
                \r\t4. ls: print all files in the filesystem
                \r\t5. store: list all files being stored in the machine
                \r\t6. get-versions: get the last N versions of the file in the machine
            """)
        
        time.sleep(1)
        # interactive shell
        self.join()
        while True:
            input_str = input("Please enter command: ")
            if input_str == 'exit':
                break
            if input_str == "1":
                local_filename = input("Enter local filename: ")
                if not os.path.exists(local_filename):
                    print("local file does not exist! please try again")
                    continue
                sdfs_filename = input("Enter SDFS filename: ")
                self.put(local_filename, sdfs_filename, HOST)
            elif input_str == "2":
                pass
            elif input_str == "3":
                pass
            elif input_str == "4":
                pass
            elif input_str == "5":
                pass
            elif input_str == "6":
                pass
            else:
                print("Invalid input. Please try again")


    def run(self):
        '''
        run function starts the server

        return: None
        '''
        logging.info('Enter run() function.')
        t_monitor = threading.Thread(target=self.monitor_program)
        t_detector = threading.Thread(target=self.detector_program)
        t_sdfs = threading.Thread(target=self.sdfs_program)
        t_shell = threading.Thread(target=self.shell)
        # t_sender = threading.Thread(target=self.send_ping)
        t_server_mp1 = threading.Thread(target = mp1_server.server_program)
        threads = []
        i = 0
        for host in utils.get_neighbors(HOST):
            t_send = threading.Thread(target=self.send_ping, args=(host, ))
            threads.append(t_send)
            i += 1
        t_monitor.start()
        t_detector.start()
        t_sdfs.start()
        t_shell.start()
        # t_sender.start()
        t_server_mp1.start()
        for t in threads:
            t.start()
        t_monitor.join()
        t_detector.join()
        t_shell.join()
        t_sdfs.join()
        # t_sender.join()
        t_server_mp1.join()
        for t in threads:
            t.join()


if __name__ == '__main__':
    s = Server()
    s.run()


#loop through mem list and find highest id when leader fails 
# everytime you store a replica, update filestructure
# where to multicast to neighbors
#  