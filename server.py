from datetime import datetime
from multiprocessing.pool import RUN
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

import signal

HOST = socket.gethostname()
IP = socket.gethostbyname(HOST)
PORT = 23120
SIZE = 4096
RUNNING = True

import time
 
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
        self.MachinesByFile = {} #dict, key = sdfs_name, value = dict (key = version, value = set of machines)
        self.FilesByMachine = {} #dict, key = hostname, value = INTERNAL sdfs_name
        self.INTRODUCER_HOST = utils.INTRODUCER_HOST

        self.send_lock = threading.Lock()
        self.recv_lock = threading.Lock()

    # multicasts the files if it is the leader, else reroute to leader
    def multicast_files(self, q1="", q2=""):
        outgoing_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # if leader -> send out files
        # if not leader -> send request to leader, and update if theyre not empty
        if HOST == self.INTRODUCER_HOST:
            query = [utils.SDFS_Type.UPDATE_FILES, self.MachinesByFile, self.FilesByMachine]
            keys = list(self.MembershipList.keys())
            for h in keys:
                if self.MembershipList[h][1] != utils.Status.LEAVE:
                    outgoing_socket.sendto(json.dumps(query).encode(), (h, PORT + 1))
        else:
            if q1 != "" and q2 != "":
                self.MachinesByFile = q1
                self.FilesByMachine = q2
            else:
                q = [utils.SDFS_Type.UPDATE_FILES, "", ""]
                outgoing_socket.sendto(json.dumps(q).encode(), (self.INTRODUCER_HOST, PORT + 1))


    def join(self):
        '''
        Contacts the introducer that the process will join the group and uptate its status.

        return: None
        '''
        timestamp = str(int(time.time()))
        join_logger.info("Encounter join before:")
        join_logger.info(self.MembershipList)

        # Clear the files directory upon joining, and re-generate the directory
        shutil.rmtree(".files", ignore_errors=True)
        os.mkdir(".files")

        shutil.rmtree("output", ignore_errors=True)
        os.mkdir("output")

        # change the status to running when it sends a message to the introducer or when it is introducer
        self.MembershipList[HOST] = (timestamp, utils.Status.RUNNING)
        join_logger.info("Encounter after before:")
        join_logger.info(self.MembershipList)
        outgoing_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if HOST != self.INTRODUCER_HOST:
            # send a message that this node wants to join to the introducer
            join_msg = [utils.Type.JOIN, HOST, self.MembershipList[HOST]]
            outgoing_socket.sendto(json.dumps(join_msg).encode(), (self.INTRODUCER_HOST, PORT))
        else:
            self.multicast_files()


    def send_ping(self, host):
        global RUNNING
        '''
        Send PING to current process's neighbor using UDP. If the host is leaved/failed, then do nothing.

        return: None
        '''
        outgoing_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while RUNNING:
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
        return 

    # Perfoms functions for PUT, GET, ROUTE, RECEIVE_FILE, UPDATE_FILES, UPDATE_PROCESS
    def sdfs_program(self):
        global RUNNING

        sdfs_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sdfs_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sdfs_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sdfs_socket.setblocking(0)
        sdfs_socket.bind((HOST, PORT + 1))
        while RUNNING:
            try:
                data, addr = sdfs_socket.recvfrom(SIZE)
                if data:
                    # assign a leader
                    self.assign_leader()
                    query = json.loads(data.decode())
                    # account for deletion of the target for an update process request
                    if query[0] == utils.SDFS_Type.UPDATE_PROCESS:
                        self.update_process(target)
                        continue
                    # delete the file from the file structure for a delete request
                    if query[0] == utils.SDFS_Type.DELETE:
                        self.delete(query[1])
                        continue
                    # call multicast with the file structures to update them for an update file request
                    if query[0] == utils.SDFS_Type.UPDATE_FILES:
                        self.multicast_files(query[1], query[2])
                        continue
                    # otherwise, perform the PUT, GET, ROUTE, RECEIVE_FILE requests
                    arg, target, local_filename, sdfs_filename = query
                    if arg == utils.SDFS_Type.PUT:
                        self.put(local_filename, sdfs_filename, target)
                    elif arg == utils.SDFS_Type.GET:
                        self.get(local_filename, sdfs_filename, target)
                    elif arg == utils.SDFS_Type.ROUTE:
                        self.route(local_filename, sdfs_filename, target)
                    elif arg == utils.SDFS_Type.RECEIVE_FILE:
                        open(sdfs_filename, "a+").close()
                        thread = threading.Thread(target = self.receive_file, args = (sdfs_filename, (target, PORT + 2)))
                        thread.start()
                    # call multicast every time an operation is done
                    self.multicast_files()
            except:
                pass
            
        return


    def detector_program(self):
        global RUNNING
        '''
        Handles receives in different situations: PING, PONG and JOIN
        When reveived PING: update membership list and send PONG back to the sender_host
        When received PONG: delete the sender_host from last_update table and update membership list
        When received JOIN: update the membership list and notify other hosts if you are the introducer host
        
        return: None
        '''
        detection_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        detection_socket.bind((HOST, PORT))
        detection_socket.setblocking(0)

        recv_logger.info('receiver program started')
        while RUNNING:
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
                        # self.MembershipList[sender_host] = (str(int(time.time())), utils.Status.RUNNING)
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
                    # elif request_type == utils.Type.FILES:
                        # retrieve file and send to sender_host(newReplicaNodeHost)
                        # self.fileStructure = request_membership
                        
                    else:
                        recv_logger.error("Unknown message type")
                    self.ml_lock.release()
            except Exception as e:
                if e.errno != 11:
                    print(e)

        return

    # assign the leader to be the node with the lowest id 
    def assign_leader(self):
        keys = sorted(self.MembershipList.keys())
        for node in keys:
            if (self.MembershipList[node][1] != utils.Status.LEAVE):
                self.INTRODUCER_HOST = node
                break
        

    def monitor_program(self):
        '''
        Monitor daemon that checks if any neighbor process has timeout

        return: None
        '''

        global RUNNING
        print("monitor started")
        while RUNNING:
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
                            # Node dies
                            # update leader
                            self.assign_leader()
                            # ensure that a new replica stores the files from the failed node
                            self.update_process(hostname)
                            monitor_logger.info("Encounter timeout after:")
                            monitor_logger.info(json.dumps(self.MembershipList))
                        self.last_update.pop(hostname, None)
                
                self.time_lock.release()
            except Exception as e:
                print(e)

        return

    # simply updates the value at the current host to a LEAVE status
    def leave(self):
        global RUNNING
        RUNNING = False
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

    # if node is the introducer/leader, delete "target" from file structures and assign a replicas for the files
    def update_process(self, target):
        outgoing_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if HOST == utils.INTRODUCER_HOST:
            # Send a bunch of reroute messages
            for internal_f in self.FilesByMachine[target]:
                # Iterate through each file in the deleted machine, and remove the machine from files
                f, v = internal_f.split('-')
                keys = self.MembershipList.keys()
                self.MachinesByFile[f][v].remove(target) # Remove the machine from the dictionary, no longer in the file

                N = len(keys)
                offset = random.randrange(N)
                for i in range(N):
                    # assign a new replica for every file 
                    new_replica = self.MembershipList[keys[(i + offset) % N]]
                    old_replica = utils.elem(self.MachinesByFile[f][v])

                    if new_replica[1] != utils.Status.LEAVE:
                        q = [utils.SDFS_Type.ROUTE, new_replica, f, f".files/{internal_f}"]
                        outgoing_socket.sendto(json.dumps(q).encode(), (old_replica, PORT + 1))
                        break
        else:
            query = [utils.SDFS_Type.UPDATE_PROCESS, target, "", ""]
            outgoing_socket.sendto(json.dumps(query).encode(), (utils.INTRODUCER_HOST, PORT + 1))


    # This will run on a thread, IF the receiver gets a SEND/GET request
    # Note that addr is the address of PORT + 2
    def receive_file(self, dest_filename, addr):
        self.recv_lock.acquire()
        recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        recv_socket.bind((HOST, PORT + 3))

        # Send ACK to sender, indicating ready to download
        recv_socket.sendto("Ready".encode(), addr)

        # Receive the size from the sender
        data, _ = recv_socket.recvfrom(SIZE)
        size = int(data.decode())
        recv_socket.sendto("Size Received".encode(), addr)
        
        f = open(f"{dest_filename}", "a+")
        data, _ = recv_socket.recvfrom(size)
        f.write(data.decode())
        f.close()

        recv_socket.sendto("File received".encode(), addr)
        recv_socket.close()
        self.recv_lock.release()

        return

    # This will also run on a thread, hosted on PORT + 2
    # Assumptions: local filename does exist, also that this is routed through the leader
    def send_file(self, source_filename, dest_filename, target):
        self.send_lock.acquire()
        send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        send_socket.bind((HOST, PORT + 2))
        recv_addr = (target, PORT + 3)

        # Send SDFS_filename to receiver, wait for ACK
        query = [utils.SDFS_Type.RECEIVE_FILE, HOST, source_filename, dest_filename]
        send_socket.sendto(json.dumps(query).encode(), (target, PORT + 1))
        data, _ = send_socket.recvfrom(SIZE)
        msg = data.decode()

        # Send the size to the target, wait for ACK
        size = os.path.getsize(source_filename)
        send_socket.sendto(str(size).encode(), recv_addr)
        data, _ = send_socket.recvfrom(SIZE)
        msg = data.decode()
        
        # Send the data to the target, wait for ACK
        with open(source_filename, "r") as f:
            send_socket.sendto(f.read().encode(), recv_addr)
        data, _ = send_socket.recvfrom(SIZE)
        msg = data.decode()

        send_socket.close()
        self.send_lock.release()
        return
        

    # If you receive a route_put: you need to send a file to the given target
    def route(self, source_filename, dest_filename, target):
        t = threading.Thread(target = self.send_file, args = (source_filename, dest_filename, target))
        t.start()

    # if the host is the introducer, remove all instances of the file from the file structures, else send a delete request to introducer
    def delete(self, sdfs_filename):
        sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if HOST != self.INTRODUCER_HOST:
            query = [utils.SDFS_Type.DELETE, sdfs_filename]
            sender_socket.sendto(json.dumps(query).encode(), (self.INTRODUCER_HOST, PORT + 1))
        else:
            for v in self.MachinesByFile[sdfs_filename]:
                for m in self.MachinesByFile[sdfs_filename][v]:
                    self.FilesByMachine[m].remove(f"{sdfs_filename}-{v}")
            self.MachinesByFile.pop(sdfs_filename)

    # to put a file, find alive nodes for the replicas, update the file at each replica
    def put(self, local_filename, sdfs_filename, target):
        sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if HOST != self.INTRODUCER_HOST:
            # Reroute the put request to the leader
            query = [utils.SDFS_Type.PUT, target, local_filename, sdfs_filename]
            sender_socket.sendto(json.dumps(query).encode(), (self.INTRODUCER_HOST, PORT + 1))
        else:
            # Generate the next node that isn't dead, given a certain offset from the memebrship list
            def get_replica_set():
                keys = list(self.MembershipList.keys())
                N = len(keys)
                curr_idx = random.randrange(N)
                end_idx = curr_idx - 1
                replica_set = set()

                ct = 4
                while ct > 0:
                    if self.MembershipList[keys[curr_idx % N]][1] != utils.Status.LEAVE:
                        replica_set.add(keys[curr_idx % N])
                        ct -= 1
                    curr_idx += 1
                    if curr_idx == end_idx:
                        break

                return replica_set

            replica_set = get_replica_set()

            # update the replicas from the generated replica set and the file structures with the respective files and replicas
            if sdfs_filename not in self.MachinesByFile:
                self.MachinesByFile[sdfs_filename] = {}
            current_version = len(self.MachinesByFile[sdfs_filename]) + 1
            self.MachinesByFile[sdfs_filename][current_version] = list(replica_set)
            for i in replica_set:
                internal_sdfs_filename = f"{sdfs_filename}-{current_version}"
                q = [utils.SDFS_Type.ROUTE, i, local_filename, f".files/{internal_sdfs_filename}"]
                # Send a route back to the sender, telling it to send the file to the given nodes
                sender_socket.sendto(json.dumps(q).encode(), (target, PORT + 1))
                if i not in self.FilesByMachine:
                    self.FilesByMachine[i] = []

                self.FilesByMachine[i].append(internal_sdfs_filename)

    # if leader, find the replica with the file and version and send a ROUTE, else send a GET to the leader
    def get(self, local_filename, sdfs_filename, target, version = 0):
        outgoing_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if HOST != self.INTRODUCER_HOST:
            query = [utils.SDFS_Type.GET, target, local_filename, sdfs_filename]
            outgoing_socket.sendto(json.dumps(query).encode(), (self.INTRODUCER_HOST, PORT + 1))
        else:
            version = len(self.MachinesByFile[sdfs_filename]) - version
            # Return if we're going back too far in time (not enough versions of the file found)
            if version <= 0 or version > len(self.MachinesByFile[sdfs_filename]):
                return
            replica_node = utils.elem(self.MachinesByFile[sdfs_filename][version]) # gets a machine with the replica on it, simply need to route
            query = [utils.SDFS_Type.ROUTE, target, f".files/{sdfs_filename}-{version}", local_filename]
            outgoing_socket.sendto(json.dumps(query).encode(), (replica_node, PORT + 1))


    def shell(self):
        global RUNNING
        print("""Please use the following codes for the below functionalities:\n
                \r\t 0. exit: exit the file system
                \r\t 1. put: add a file to the file system
                \r\t 2. get: get a file from the file system
                \r\t 3. delete: delete a file from the file system
                \r\t 4. ls: print all files in the filesystem
                \r\t 5. store: list all files being stored in the machine
                \r\t 6. get-versions: get the last N versions of the file in the machine
                \r\t 7. list_mem: list the membership list"
                \r\t 8. list_self: list self's id"
                \r\t 9. leave: command to voluntarily leave the group (different from a failure, which will be Ctrl-C or kill)"
            """)
        
        time.sleep(1)
        self.join()
        while RUNNING:
            self.assign_leader()
            self.multicast_files()

            input_str = input("Please enter command: ")
            start = datetime.now()
            if input_str == "0" or input_str == "exit":
                RUNNING = False
                break
            elif input_str == "1":
                print("Selected put")
                local_filename = input("Enter local filename: ")
                if not os.path.exists(local_filename):
                    print("local file does not exist! please try again")
                    continue
                sdfs_filename = input("Enter SDFS filename: ")
                self.put(local_filename, sdfs_filename, HOST)
            elif input_str == "2":
                print("Selected get")
                sdfs_filename = input("Enter SDFS filename: ")
                if not sdfs_filename in self.MachinesByFile:
                    print("file does not exist in the system! please try again")
                    continue
                local_filename = input("Enter local filename: ")
                open(local_filename, "w+").close()
                self.get(local_filename, sdfs_filename, HOST)
            elif input_str == "3":
                print("Selected delete")
                sdfs_filename = input("Enter SDFS filename: ")
                if not sdfs_filename in self.MachinesByFile:
                    print("file does not exist in the system! please try again")
                    continue
                self.delete(sdfs_filename)
            elif input_str == "4":
                print("Selected ls")
                sdfs_filename = input("Enter SDFS filename: ")
                if not sdfs_filename in self.MachinesByFile:
                    print("file does not exist in the system! please try again")
                    continue
                print(self.MachinesByFile[sdfs_filename])
            elif input_str == "5":
                print("Selected store")
                print(self.FilesByMachine.get(HOST, []))
            elif input_str == "6":
                print("Selected num_versions")

                sdfs_filename = input("Enter SDFS filename: ")
                # Clear the files directory upon joining, and re-generate the directory
                if not sdfs_filename in self.MachinesByFile:
                    print("file does not exist in the system! please try again")
                    continue

                local_filename = input("Enter local directory: ")
                num_versions = int(input("Enter num versions: " ))

                shutil.rmtree(f"output/{local_filename}", ignore_errors=True)
                os.mkdir(f"output/{local_filename}")

                for i in range(num_versions):
                    self.get(f"output/{local_filename}/{sdfs_filename}-{i}", sdfs_filename, HOST, i)

                while (len(os.listdir(f"output/{local_filename}")) != min(num_versions, len(self.MachinesByFile[sdfs_filename]))):
                    pass
                
            elif input_str == "7":
                print("Selected list_mem")
                self.print_membership_list()
            elif input_str == "8":
                print("Selected list_self")
                self.print_self_id()
            elif input_str == "9":
                print("Selected leave")
                self.leave()
            else:
                print(f"Invalid input \"{input_str}\". Please try again")
                continue

            print(f"Finished operation! Time taken: {datetime.now() - start}")
        return


    def run(self):
        global RUNNING
        '''
        run function starts the server

        return: None
        '''
        logging.info('Enter run() function.')
        t_monitor = threading.Thread(target=self.monitor_program)
        t_detector = threading.Thread(target=self.detector_program)
        t_sdfs = threading.Thread(target=self.sdfs_program)
        t_shell = threading.Thread(target=self.shell)
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
        for t in threads:
            t.start()
        t_monitor.join()
        t_detector.join()
        t_shell.join()
        t_sdfs.join()
        for t in threads:
            t.join()
        print("PROGRAM FINISHED.")
        return
if __name__ == '__main__':
    s = Server()
    s.run()

