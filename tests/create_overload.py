import argparse
import json
import string
import random
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--n_reqs", required=False, default=20, type=int)
parser.add_argument("-w", "--workers", required=False, default=20, type=int)

args = vars(parser.parse_args())
NUM_OF_REQS = int(args["n_reqs"])
NUM_OF_WORKERS = int(args["workers"])

CLUSTER_SIZE = 5
HOSTS = ["192.168.1.12{}".format(idx) for idx in range(CLUSTER_SIZE)]
PORT = 9000

class PerformanceMetrics:
    def __init__(self, success, value, resp_time) -> None:
        self.success = success # 1: Success, 0: Failure
        self.value = value # Replica num in this case
        self.resp_time = resp_time # Response time

KEY_LEN = 16
LETTERS = string.ascii_lowercase
MAX_VAL = 1e+4


def __generate_random_request() -> str:
    key = "".join(random.choice(LETTERS) for _ in range(KEY_LEN))
    value = random.choice(range(MAX_VAL))
    return " ".join(["create", key, value])


def send_create_request(request:str) -> PerformanceMetrics:
    user_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    dst_addr = (random.choice(HOSTS), PORT)

    # Timer starts
    start_time = time.perf_counter()

    # Send and receive
    user_socket.connect(dst_addr)
    user_socket.send(request.encode("utf-8"))
    ack = dict(json.loads(user_socket.recv(1024).decode("utf-8")))
    user_socket.close()

    # Timer ends
    duration = time.perf_counter() - start_time
    return PerformanceMetrics(ack.get("success"), ack.get("value"), duration)


def print_results(response_list:List[PerformanceMetrics]) -> None:
    total_delay = success_count = failure_count = replicas = 0

    for response in response_list:
        if response.success: success_count += 1
        else: failure_count += 1
        replicas += response.value
        total_delay += response.resp_time
    
    print("Request Count : {}".format(NUM_OF_REQS))
    print("Average Delay : {}".format(total_delay / NUM_OF_REQS))
    print("Success Rate  : {}".format(success_count / NUM_OF_REQS))
    print("Failure Rate  : {}".format(failure_count / NUM_OF_REQS))
    print("Replica Count : {}/{}".format(replicas, 3*NUM_OF_REQS))


if __name__ == "__main__":
    request_list = [__generate_random_request() for _ in range(NUM_OF_REQS)]
    response_list:List[PerformanceMetrics] = list()

    with ThreadPoolExecutor(max_workers=NUM_OF_WORKERS) as pool:
        response_list = pool.map(send_create_request, request_list)

    print_results(response_list)
    