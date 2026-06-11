import os
import sys
import json
import subprocess
from optparse import OptionParser

__version__="1.0"
__status__ = "Dev"



###############################
def main():


    usage = "\n%prog  [options]"
    parser = OptionParser(usage,version="%prog version___")
    parser.add_option("-s","--server",action="store",dest="server",help="dev/tst/beta/prd")
    parser.add_option("-a","--apiname",action="store",dest="apiname",help="") 
    (options,args) = parser.parse_args()

    for key in ([options.server, options.apiname]):
        if not (key):
            parser.print_help()
            sys.exit(0)

    server = options.server
    api_name = options.apiname

    config_obj = json.loads(open("conf/curl.json", "r").read())
    auth_obj = json.loads(open("auth/auth.json", "r").read())

    cn_type = "Content-Type: application/json"
    api_url = "http://localhost:%s/auth/login" % (config_obj["port"][server])
    auth_obj_str = json.dumps(auth_obj)

    cmd = "curl -H '%s' %s -d '%s' -o tmp/response.txt" % (cn_type, api_url, auth_obj_str )
    res = subprocess.getoutput(cmd)
    res_json = json.loads(open("tmp/response.txt", "r").read()) 
    TOKEN = res_json["access_token"]

    api_url = config_obj["api"][api_name]["url"]  % (config_obj["port"][server])
    query_file = config_obj["api"][api_name]["file"]
    query_obj = json.loads(open(query_file, "r").read())
    query_obj_str = json.dumps(query_obj)

    cmd = "curl -H '%s' -H 'Authorization: Bearer %s' %s -d '%s' " % (cn_type, TOKEN, api_url, query_obj_str )
    #res = subprocess.getoutput(cmd)
    #print (res)
    print (cmd)

    return


if __name__ == '__main__':
    main()


