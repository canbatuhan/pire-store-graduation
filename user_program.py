import socket
from typing import Tuple

from pire.util.constants import ENCODING

user_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
user_socket.bind(("127.0.0.1", 9000))

def get_input() -> int:
    print("-"*40)
    print("Select client to send request:")
    for count in range(5):
        print("{}. PiRe-0{}".format(count, count))
    return int(input("Selection: "))

def get_address(pire_id:int) -> Tuple[str,int]:
    return "127.0.0.1", int("80{}5".format(pire_id))

while True:
    pire_id = get_input()
    host, port = get_address(pire_id)
    try: # Try to connect
        user_socket.connect((host, port))
        while True:
            try:
                request = input("Your request: ")
                user_socket.send(request.encode(ENCODING))
                if request == "exit":
                    break
                ack = user_socket.recv(1024)
                print("ACK = {}".format(ack.decode(ENCODING)))
            except:
                break
    except: # Failed to connect
        print("Node not available")