import sys 
from optparse import OptionParser
import json 
import subprocess 

def main():
    
    # defines the foramt of the command line prompt 
    usage = 'python %prog [options]' 
    # option parser 
    parser = OptionParser(usage)
    # add new command line option, either -s or --server, which takes the values of dev, tst, beta, or prd 
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

    # read in config file
    with open('config.json', 'r') as f:
        config_obj = json.load(f)
    
    # create the string for the Docker image name
    api_image = f"{config_obj['project']}_api_{server}"
    # create the string for the container name 
    api_container_name = f'running_{api_image}'
    # mongo container name 
    mongo_container_name = f"running_{config_obj['project']}_mongo_{server}" 
    # api port 
    api_port = config_obj['api_port'][server]

    # database name 
    db_name = config_obj['dbinfo']['dbname']
    # data path 
    data_path = config_obj['data_path']
    # network name 
    mongo_network_name = f"{config_obj['dbinfo']['bridge_network']}_{server}"
    # grab user username and password 
    db_user = config_obj['dbinfo'][db_name]['user']
    db_pass = config_obj['dbinfo'][db_name]['password']

    # mail configuration (not used right now)
    # mail_server = config_obj['mail']['server']
    # mail_port = config_obj['mail']['port']
    # mail_sender = config_obj['mail']['sender']

    # mongodb connection string
    conn_str = f'mongodb://{db_user}:{db_pass}@{mongo_container_name}:27017/?authSource={db_name}'

    # command list 
    cmd_list = []
    # command to package the api 
    cmd_list.append('python setup.py bdist_wheel')
    # if no python error, use this
    # cmd_list.append('python3 setup.py bdist_wheel')
    # command to build a docker image from the dockerfile 
    cmd_list.append(f'docker build -t {api_image} .')
    
    # create the command to delete the api container if it already exists 
    container_id = subprocess.getoutput(f'docker ps --all | grep {api_container_name}').split(' ')[0].strip()
    if container_id.strip() != '':
        cmd_list.append(f'docker rm -f {container_id}')
    
    # create the command to create the api docker container 
    cmd = f'docker create --name {api_container_name} --network {mongo_network_name} -p 127.0.0.1:{api_port}:80'
    cmd += f' -v {data_path}:{data_path} -v /software/pipes:/hostpipe -e MONGODB_CONNSTRING={conn_str} -e DB_NAME={db_name}'
    cmd += f' -e DATA_PATH={data_path} -e SERVER={server} {api_image}'
    # -e MAIL_SERVER={mail_server} -e MAIL_PORT={mail_port} -e MAIL_SENDER={mail_sender}
    cmd_list.append(cmd)

    def run_command(cmd):
        result = subprocess.run(cmd, shell = True, text = True, encoding = 'utf-8', errors = 'replace', capture_output = True)
        # for python 3.6 and below
        # result = subprocess.run(cmd, shell = True, universal_newlines = True, errors = 'replace', stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        if result.returncode != 0:
            print(f'Command failed with error code {result.returncode}: {result.stderr}')
        else:
            print(result.stdout)

    # run the commands 
    for cmd in cmd_list:
        run_command(cmd)

if __name__ == '__main__':
    main() 