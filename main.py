import argparse
from pire.client import PireClient

parser = argparse.ArgumentParser()
parser.add_argument("-id", "--id", required=False, default="PiRe-00", type=str)
args = vars(parser.parse_args())
PIRE_ID = args["id"]

client = PireClient(PIRE_ID)
if __name__ == "__main__":
    client.start()
    client.run()