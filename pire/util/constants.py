# Client Constants
CLIENT_CONFIG_PATH = "pire/configuration/config.json"
LOCAL_DB_PATH = lambda client_id : "pire/docs/databases/{}_local.db".format(client_id)
LOG_FILE_PATH = lambda client_id : "pire/docs/logs/{}_log.txt".format(client_id)

# Cluster Constants
N_REPLICAS = 4

# State Machine Constants
INITIAL_POLL_TIME = 5.2e-9 # 52 nanoseconds
MAX_POLL_TIME = 5 # 5 seconds

# User Interface Constants
BUFFER_SIZE = 256
ENCODING = "utf-8"