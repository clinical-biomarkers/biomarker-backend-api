import sys
import argparse
import subprocess
import misc_functions as misc_fns


def main():

    ### handle command line arguments
    parser = argparse.ArgumentParser(
        prog="create_mongodb_container.py", usage="python load_data.py [options] server"
    )
    parser.add_argument("-s", "--server", help="tst/prd")
    options = parser.parse_args()
    if not options.server or options.server not in {"tst", "prd"}:
        parser.print_help()
        sys.exit(1)
    server = options.server

    ### get config info for docker container creation
    config_obj = misc_fns.load_json("config.json")
    if not isinstance(config_obj, dict):
        print(
            f"Error reading config JSON, expected type dict and got {type(config_obj)}."
        )
        sys.exit(1)
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
    mongo_cmd += f" -v {data_path}/api_db/{server}:/data/db {e_params} mongo"
    cmd_list.append(mongo_cmd)

    # run the commands
    for cmd in cmd_list:
        x = subprocess.getoutput(cmd)
        print(x)


if __name__ == "__main__":
    main()
