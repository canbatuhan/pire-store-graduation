import os
import sys
import argparse
import asyncio
from pire.node import PireNode

# Close terminal
"""f = open(os.devnull, 'w')
sys.stdout = f
sys.stderr = f"""

# Parse the arguments
parser = argparse.ArgumentParser("pire-store HTTP Server for user interaction")
parser.add_argument("-config", default="./docs/sample.yaml")
args = vars(parser.parse_args())
CONFIG_PATH = args["config"]

# Create Node
node = PireNode(CONFIG_PATH)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(node.main())
    