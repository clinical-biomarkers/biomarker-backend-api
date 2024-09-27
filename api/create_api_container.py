import subprocess
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.parser import standard_parser, parse_server
from tutils.config import get_config


def main() -> None:

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    ### get config info for docker container creation
    config_obj = get_config()
    api_image = f"{config_obj['project']}_api_{server}"
    api_container_name = f"running_{api_image}"
    mongo_container_name = f"running_{config_obj['project']}_mongo_{server}"
    api_port = config_obj["api_port"][server]
    db_name = config_obj["dbinfo"]["dbname"]
    data_path = config_obj["data_path"]
    mongo_network_name = f"{config_obj['dbinfo']['bridge_network']}_{server}"
    db_user = config_obj["dbinfo"][db_name]["user"]
    db_pass = config_obj["dbinfo"][db_name]["password"]
    conn_str = f"mongodb://{db_user}:{db_pass}@{mongo_container_name}:27017/?authSource={db_name}"

    ### create and populate command list
    cmd_list = []

    # command to package the api
    cmd_list.append("python setup.py bdist_wheel")
    # if no python error, use below line
    # cmd_list.append('python3 setup.py bdist_wheel')

    cmd_list.append(f"docker build -t {api_image} .")

    # create the command to delete the api container if it already exists
    container_id = (
        subprocess.getoutput(f"docker ps --all | grep {api_container_name}")
        .split(" ")[0]
        .strip()
    )
    if container_id.strip() != "":
        cmd_list.append(f"docker rm -f {container_id}")

    # create the command to create the api docker container
    cmd = f"docker create --name {api_container_name} --network {mongo_network_name} -p 127.0.0.1:{api_port}:80"
    cmd += f" -v {data_path}:{data_path} -v /software/pipes:/hostpipe -e MONGODB_CONNSTRING={conn_str} -e DB_NAME={db_name}"
    cmd += f" -e DATA_PATH={data_path} -e SERVER={server} {api_image}"
    cmd_list.append(cmd)

    def run_command(cmd):
        result = subprocess.run(
            cmd,
            shell=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
        )
        # for python 3.6 and below, use below line
        # result = subprocess.run(cmd, shell = True, universal_newlines = True, errors = 'replace', stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        if result.returncode != 0:
            print(
                f"Command failed with error code {result.returncode}: {result.stderr}"
            )
        else:
            print(result.stdout)

    # run the commands
    for cmd in cmd_list:
        run_command(cmd)


if __name__ == "__main__":
    main()
