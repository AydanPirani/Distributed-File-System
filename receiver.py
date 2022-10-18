import logging
import socket
import json
from threading import Lock
import utils
import time

init = False
HOST = socket.gethostname()
IP = socket.gethostbyname(HOST)
PORT = 23333
tmp_timestamp = str(int(time.time()))
lock = Lock()
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=HOST + '.log',
                    filemode='w')
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)

# Now, we can log to the root logger, or any other logger. First the root...
logging.info('Jackdaws love my big sphinx of quartz.')
logger1 = logging.getLogger('receiver')

membership_list = {
    HOST: (tmp_timestamp, utils.Status.LEAVE),
    HOST: (tmp_timestamp, "test")
}
# the last local update time for the other 9 processes
# example: {id , last local time}
last_update = {}


def receiver_program():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((HOST, PORT))
    logger1.info('[INFO]: receiver program started')
    print("started")
    while True:
        data, addr = s.recvfrom(4096)
        print("data", data)
        print("addr", addr)
        logging.info("[INFO]: connection from: " + str(addr))
        request = data.decode()
        print("request", request)
        if request:
            request_dict = json.loads(request)
            # recv message from sender validates the sender is running
            last_update[socket.gethostbyaddr(addr[0])[0]] = time.time()
            print(last_update)
            # two cases:
            # 1. new joiner requests the introducer to give full membership list
            if utils.is_introducer(HOST) and len(request_dict) == 1:
                logging.info("[INFO]: introducer recv connection from new joiner: " + str(addr))
                for sender_host, value in request_dict.items():
                    timestamp, status = value[0], value[1]
                    print("conn addr: ", addr)
                    print("sender_host :", sender_host)
                    # send the new joiner the membership list
                    if status == utils.Status.NEW:
                        lock.acquire()
                        membership_list[sender_host] = (timestamp, utils.Status.RUNNING)  # update its membership list
                        s.sendto(json.dumps(membership_list).encode(), addr)
                        lock.release()
                    else:
                        logging.info("[ERROR]: wrong new joiner status since the status should be NEW")
            # 2. the sender process pings the receiver, and sends us its full membership list
            else:
                logging.info("[INFO]: recv connection from a peer: " + str(addr))
                ack_dict = {"ack": True}
                s.sendto(json.dumps(ack_dict).encode(), addr)
                lock.acquire()
                for sender_host, value in request_dict.items():
                    timestamp, status = value[0], value[1]
                    # if process is running, last_update_time == now!
                    if status == utils.Status.RUNNING:
                        last_update[sender_host] = time.time()
                    membership_list[sender_host] = (timestamp, status)
                lock.release()

    conn.close()


def monitor_program():
    while True:
        for hostname, last_update_time in last_update.items():
            # if the last update time was at least 1.5s ago, mark that process as time out
            if time.time() - last_update_time >= 1.5:
                logging.INFO('[INFO] ' + hostname + ' has timed out')
                lock.acquire()
                prev_timestamp = membership_list[hostname][0]
                membership_list[hostname] = (prev_timestamp, utils.Status.LEAVE)
                lock.release()
        print("print membership list")
        print(membership_list)
        print("print last timestamp")
        print(last_update)


if __name__ == '__main__':
    receiver_program()
