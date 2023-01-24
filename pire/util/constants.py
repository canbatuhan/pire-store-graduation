# Client Constants
CLIENT_CONFIG_PATH = "pire/configuration/config.json"
LOCAL_DB_PATH = lambda client_id : "pire/docs/databases/{}_local.db".format(client_id)
LOG_FILE_PATH = lambda client_id : "pire/docs/logs/{}_log.txt".format(client_id)

# User Interface Constants
BUFFER_SIZE = 256
ENCODING = "utf-8"