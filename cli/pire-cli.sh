#!/bin/bash

addresses=("192.168.1.120" "192.168.1.121" "192.168.1.122" "192.168.1.123" "192.168.1.124")

select_random_address() {
    local num_addresses=${#addresses[@]}
    local random_index=$((RANDOM % num_addresses))
    echo "${addresses[random_index]}"
}

send_request() {
    local address=$1
    local path=$2
    local method=$3
    local key=$4
    local value=$5
    curl -X "$method" "http://$address$path" -d "key=$key&value=$value"
}

while true; do
    read -rp "pire-store > " command key value

    case $command in
        help)
            echo "------------- pire-store HTTP Client -------------"
            echo "A Command-line Interface for sending HTTP requests"
            echo "to the pire-store cluster."
            echo ""
            echo "Commands:"
            echo "  - help : Prints the manual of the CLI."
            echo ""
            echo "  - create <key> <value> : Sends a request to create"
            echo " a pair with the given key and value."
            echo ""
            echo "  - read <key> : Sends a request to read the pair"
            echo " with the given key."
            echo ""
            echo "  - update <key> <value> : Sends a request to update"
            echo " the pair with the given key and value."
            echo ""
            echo "  - delete <key> : Sends a request to delete the pair"
            echo " with the given key."
            echo ""
            echo "  - exit : Terminates the CLI."
            echo "--------------------------------------------------"
            ;;

        create)
            address=$(select_random_address)
            send_request "$address" "/pire/kv/create" "PUT" "$key" "$value"
            ;;

        read)
            address=$(select_random_address)
            send_request "$address" "/pire/kv/read" "PUT" "$key"
            ;;

        update)
            address=$(select_random_address)
            send_request "$address" "/pire/kv/update" "PUT" "$key" "$value"
            ;;

        delete)
            address=$(select_random_address)
            send_request "$address" "/pire/kv/delete" "PUT" "$key"
            ;;

        exit)
            break
            ;;

        *)
            echo "Invalid command. Available commands: create, read, update, delete"
            ;;
    esac
done