# Client Constants
CLIENT_CONFIG_PATH = "pire/configuration/config.json"
LOCAL_DB_PATH = "pire/docs/local.db"
LOG_FILE_PATH = "pire/docs/log.txt"

# Cluster Constants
MAX_ID = 1E+4
N_REPLICAS = 3
N_SERVICER = 10
MIN_REPLICAS = 1

# State Machine Constants
INITIAL_POLL_TIME = 5.2e-9 # 52 nanoseconds
MAX_POLL_TIME = 0.050 # 50 milliseconds

# Database Constants
MIN_DUMP_TIMEOUT = 0.025 # 25 milliseconds
MAX_DUMP_TIMEOUT = 2 # 2 seconds

# User Interface Constants
N_HANDLER = 10
BUFFER_SIZE = 256
ENCODING = "utf-8"