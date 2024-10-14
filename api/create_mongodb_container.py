import subprocess
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.parser import standard_parser, parse_server
from tutils.config import get_config

MONGO_MEM = 524288000  # in bytes


def main() -> None:

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    ### get config info for docker container creation
    config_obj = get_config()
    api_container_name = f"running_{config_obj['project']}_api_{server}"
    mongo_container_name = f"running_{config_obj['project']}_mongo_{server}"
    mongo_network_name = f"{config_obj['dbinfo']['bridge_network']}_{server}"
    mongo_port = config_obj["dbinfo"]["port"][server]
    data_path = config_obj["data_path"]
    username = config_obj["dbinfo"]["admin"]["user"]
    password = config_obj["dbinfo"]["admin"]["password"]
    e_params = f"-e MONGO_INITDB_ROOT_USERNAME={username} -e MONGO_INITDB_ROOT_PASSWORD={password}"

    ### create and populate command list
    cmd_list = []

    # check if containers already exist (whether running or in a stopped state)
    for c in {api_container_name, mongo_container_name}:
        cmd = f"docker ps --all | grep {c}"
        container_id = subprocess.getoutput(cmd).split(" ")[0].strip()
        if container_id.strip() != "":
            print(f"Found container: {c}")
            cmd_list.append(f"docker rm -f {container_id}")

    # check if docker network already exists
    network_cmd = f"docker network ls | grep {mongo_network_name}"
    network_cmd_output = subprocess.getoutput(network_cmd).split()
    if network_cmd_output != []:
        if network_cmd_output[1] == mongo_network_name:
            print(f"Found network: {network_cmd_output[1]}")
            cmd_list.append(f"docker network rm {mongo_network_name} | true")

    # create docker network command
    cmd_list.append(f"docker network create -d bridge {mongo_network_name}")

    # create mongo container command
    # the external port is dynamically assigned using mongo_port, and the container's port of 27017 is used because that is the default port that MongoDB listens on inside the container
    mongo_cmd = f"docker create --name {mongo_container_name} --network {mongo_network_name} -p 127.0.0.1:{mongo_port}:27017"
    mongo_cmd += f" -v {data_path}/api_db/{server}:/data/db {e_params} mongo "
    mongo_cmd += f"mongod --setParameter internalQueryMaxBlockingSortMemoryUsageBytes={MONGO_MEM}"
    cmd_list.append(mongo_cmd)

    # run the commands
    for cmd in cmd_list:
        x = subprocess.getoutput(cmd)
        print(x)


if __name__ == "__main__":
    main()
