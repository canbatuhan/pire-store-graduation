import argparse
import socket
import random
import time
import json

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--cmd", required=False, default="create", type=str)
parser.add_argument("-k", "--key", required=False, default="x", type=str)
parser.add_argument("-v", "--val", required=False, default="42", type=str)

args = vars(parser.parse_args())
COMMAND = args["cmd"]
KEY = args["key"]
VALUE = args["val"]

CLUSTER_SIZE = 5
HOSTS = ["192.168.1.12{}".format(idx) for idx in range(CLUSTER_SIZE)]
PORT = 9000

if __name__ == "__main__":
    user_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    dst_addr = (random.choice(HOSTS), PORT)
    request = " ".join([COMMAND, KEY, VALUE])

    # Timer starts
    start_time = time.perf_counter()

    # Send and receive
    user_socket.connect(dst_addr)
    user_socket.send(request.encode("utf-8"))
    ack = dict(json.loads(user_socket.recv(1024).decode("utf-8")))
    user_socket.close()

    # Timer ends
    duration = time.perf_counter() - start_time

    # Print output
    print("Response : {}".format(ack.get("success")))
    print("Value    : {}".format(ack.get("value")))