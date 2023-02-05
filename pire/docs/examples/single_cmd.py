import argparse
import socket

parser = argparse.ArgumentParser()
parser.add_argument("--host", type=str, required=False, default="127.0.0.1")
parser.add_argument("--port", type=int, required=True)
parser.add_argument("--cmd", type=str, required=True)

args = vars(parser.parse_args())
HOST = str(args["host"])
PORT = int(args["port"])
CMD = str(args["cmd"])

if __name__ == "__main__":
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 9000))

    s.connect((HOST,PORT))
    s.send(CMD.encode("utf-8"))
    ack = s.recv(1024).decode("utf-8")
    
    print(ack)
    s.close()