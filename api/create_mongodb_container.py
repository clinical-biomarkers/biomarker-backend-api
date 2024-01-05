import sys
from optparse import OptionParser
import json 
import subprocess

def main(): 
    
    # defines the format of the command line prompt 
    usage = 'python %prog [options]'
    # option parser 
    parser = OptionParser(usage)
    # add new command line option, either -s or --server, which takes values of dev, tst, beta, or prd
    # stores the server value in parser.server 
    parser.add_option('-s', '--server', action = 'store', dest = 'server', help = 'dev/tst/beta/prd')
    # parse the command line arguments
    (options, _) = parser.parse_args()

    # check the input arguments
    if not options.server:
        parser.print_help()
        sys.exit(1)
    if options.server not in {'dev', 'tst', 'beta', 'prd'}:
        parser.print_help()
        sys.exit(1)
    server = options.server

    # read in the config file 
    with open('config.json', 'r') as f:
        config_obj = json.load(f)
    
    # construct docker container names
    api_container_name = f"running_{config_obj['project']}_api_{server}"
    mongo_container_name = f"running_{config_obj['project']}_mongo_{server}"
    # docker network name 
    mongo_network_name = f"{config_obj['dbinfo']['bridge_network']}_{server}"
    
    # mongo database port 
    mongo_port = config_obj['dbinfo']['port'][server]
    # path to the data 
    data_path = config_obj['data_path']

    # grab admin db username and password 
    username = config_obj['dbinfo']['admin']['user']
    password = config_obj['dbinfo']['admin']['password']
    e_params = f'-e MONGO_INITDB_ROOT_USERNAME={username} -e MONGO_INITDB_ROOT_PASSWORD={password}'

    # command list
    cmd_list = []

    # check if containers already exist (whether running or in a stopped state)
    for c in {api_container_name, mongo_container_name}:
        cmd = f'docker ps --all | grep {c}'
        container_id = subprocess.getoutput(cmd).split(' ')[0].strip()
        if container_id.strip() != '':
            print(f'Found container: {c}')
            cmd_list.append(f'docker rm -f {container_id}')

    # check if docker network already exists
    network_cmd = f'docker network ls | grep {mongo_network_name}'
    network_cmd_output = subprocess.getoutput(network_cmd).split() 
    if network_cmd_output != []: 
        if network_cmd_output[1] == mongo_network_name:
            print(f'Found network: {network_cmd_output[1]}')
            cmd_list.append(f'docker network rm {mongo_network_name} | true')
    
    # create docker network command 
    cmd_list.append(f'docker network create -d bridge {mongo_network_name}')

    # create mongo container command 
    # the external port is dynamically assigned using mongo_port, and the container's port of 27017 is used because that is the default port that MongoDB listens on inside the container
    mongo_cmd = f'docker create --name {mongo_container_name} --network {mongo_network_name} -p 127.0.0.1:{mongo_port}:27017'
    mongo_cmd += f' -v {data_path}/api_db/{server}:/data/db {e_params} mongo'
    cmd_list.append(mongo_cmd)

    # run the commands 
    for cmd in cmd_list:
        x = subprocess.getoutput(cmd)
        print(x)

if __name__ == '__main__':
    main()