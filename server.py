import socket
import time
import threading
import json
import utils
import logging
from logging.handlers import RotatingFileHandler
import datetime
import mp1_client
import mp1_server

HOST = socket.gethostname()
IP = socket.gethostbyname(HOST)
PORT = 23333

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

    def join(self):
        '''
        Contacts the introducer that the process will join the group and uptate its status.

        return: None
        '''
        print("start joining")
        timestamp = str(int(time.time()))
        join_logger.info("Encounter join before:")
        join_logger.info(self.MembershipList)
        self.MembershipList[HOST] = (timestamp, utils.Status.RUNNING)
        join_logger.info("Encounter after before:")
        join_logger.info(self.MembershipList)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if HOST != utils.INTRODUCER_HOST:
            join_msg = [utils.Type.JOIN, HOST, self.MembershipList[HOST]]
            s.sendto(json.dumps(join_msg).encode(), (utils.INTRODUCER_HOST, PORT))
        else:
            print("This is introducer host!")

    def send_ping(self, host):
        '''
        Send PING to current process's neighbor using UDP. If the host is leaved/failed, then do nothing.

        return: None
        '''
        print("sender started")
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while True:
            time.sleep(0.3)
            if self.MembershipList[HOST][1] == utils.Status.LEAVE or host not in self.MembershipList or self.MembershipList[host][1] == utils.Status.LEAVE:
                continue
            try:
                self.ml_lock.acquire()
                timestamp = str(int(time.time()))
                send_logger.info("Encounter send before:")
                send_logger.info(self.MembershipList)
                self.MembershipList[HOST] = (timestamp, utils.Status.RUNNING)
                send_logger.info("Encounter send after:")
                send_logger.info(self.MembershipList)
                
                ping_msg = [utils.Type.PING, HOST, self.MembershipList]
                s.sendto(json.dumps(ping_msg).encode(), (host, PORT))
                if host in self.MembershipList and host not in self.last_update:
                    self.time_lock.acquire()
                    self.last_update[host] = time.time()
                    self.time_lock.release()
                self.ml_lock.release()
            except Exception as e:
                print(e)

    def receiver_program(self):
        '''
        Handles receives in different situations: PING, PONG and JOIN
        When reveived PING: update membership list and send PONG back to the sender_host
        When received PONG: delete the sender_host from last_update table and update membership list
        When received JOIN: update the membershi list and notify other hosts if you are the introducer host
        
        return: None
        '''
        print("receiver started")
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((HOST, PORT))
        recv_logger.info('receiver program started')
        while True:
            try:
                if self.MembershipList[HOST][1] == utils.Status.LEAVE:
                    recv_logger.info("skip receiver program since " + HOST + " is leaved")
                    continue
                data, addr = s.recvfrom(4096)
                recv_logger.info("connection from: " + str(addr) + " with data: " + data.decode())
                if data:
                    request = data.decode()
                    request_list = json.loads(request)
                    sender_host = request_list[1]
                    request_type = request_list[0]
                    
                    request_membership = request_list[2]
            
                    self.ml_lock.acquire()
                    if request_type == utils.Type.JOIN:
                        recv_logger.info("Encounter join before:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        
                        self.MembershipList[sender_host] = (str(int(time.time())), utils.Status.NEW)
                        recv_logger.info("Encounter join after:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        
                        if HOST == utils.INTRODUCER_HOST:
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
                        for host, value in request_membership.items():
                            timestamp, status = value[0], value[1]
                            if status == utils.Status.LEAVE:
                                self.MembershipList[host] = value

                            if host not in self.MembershipList:
                                self.MembershipList[host] = value
                                continue
                        
                            if int(timestamp) > int(self.MembershipList[host][0]):
                                self.MembershipList[host] = (timestamp, status)
                        recv_logger.info("Encounter PING after:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        pong = [utils.Type.PONG, HOST, self.MembershipList[HOST]]
                        
                        s.sendto(json.dumps(pong).encode(), (sender_host, PORT))

                    elif request_type == utils.Type.PONG:
                        recv_logger.info("Encounter PONG before:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        self.MembershipList[sender_host] = request_membership
                        if sender_host in self.last_update:
                            self.time_lock.acquire()
                            self.last_update.pop(sender_host, None)
                            self.time_lock.release()
                        recv_logger.info("Encounter PONG after:")
                        recv_logger.info(json.dumps(self.MembershipList))
                    else:
                        recv_logger.error("Unknown message type")
                    self.ml_lock.release()
            except Exception as e:
                print(e)

    def monitor_program(self):
        '''
        Monitor daemon that checks if any neighbor process has timeout

        return: None
        '''
        print("monitor started")
        while True:
            try:
                self.time_lock.acquire()
                
                keys = list(self.last_update.keys())
                for hostname in keys:
                    if time.time() - self.last_update[hostname] > 2:
                        value = self.MembershipList.get(hostname, "*")
                        if value != "*" and value[1] != utils.Status.LEAVE:
                            monitor_logger.info("Encounter timeout before:")
                            monitor_logger.info(json.dumps(self.MembershipList))
                            self.MembershipList[hostname] = (value[0], utils.Status.LEAVE)
                            monitor_logger.info("Encounter timeout after:")
                            monitor_logger.info(json.dumps(self.MembershipList))
                        self.last_update.pop(hostname, None)
                
                self.time_lock.release()
            except Exception as e:
                print(e)

    
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
        
    def shell(self):
        print("Welcome to the interactive shell for CS425 MP2. You may press 1/2/3/4 for below functionalities.\n"
              "1. list_mem: list the membership list\n"
              "2. list_self: list self's id\n"
              "3. join: command to join the group\n"
              "4. leave: command to voluntarily leave the group (different from a failure, which will be Ctrl-C or kill)\n"
              "5. grep: get into mp1 grep program"
              )
        
        time.sleep(1)
        # interactive shell
        while True:
            input_str = input("Please enter input: ")
            if input_str == 'exit':
                break
            if input_str == "1":
                print("Selected list_mem")
                self.print_membership_list()
            elif input_str == "2":
                print("Selected list_self")
                self.print_self_id()
            elif input_str == "3":
                print("Selected join the group")
                self.join()
            elif input_str == "4":
                print("Selected voluntarily leave")
                self.leave()
            elif input_str == "5":
                input_command = input("Please enter grep command: ")
                c = mp1_client.Client(input_command)
                t = threading.Thread(target = c.query)
                t.start()
                t.join()
            else:
                print("Invalid input. Please try again")

    def run(self):
        '''
        run function starts the server

        return: None
        '''
        logging.info('Enter run() function.')
        t_monitor = threading.Thread(target=self.monitor_program)
        t_receiver = threading.Thread(target=self.receiver_program)
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
        t_receiver.start()
        t_shell.start()
        # t_sender.start()
        t_server_mp1.start()
        for t in threads:
            t.start()
        t_monitor.join()
        t_receiver.join()
        t_shell.join()
        t_sender.join()
        t_server_mp1.join()
        for t in threads:
            t.join()


if __name__ == '__main__':
    s = Server()
    s.run()
