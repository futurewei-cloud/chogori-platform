#!/usr/bin/python

import os
import sys

KEY_NODE_COUNT    ='NODE_COUNT'
KEY_BIND_IP       ='BIND_IP'
KEY_FIRST_TCP_PORT='FIRST_TCP_PORT'
KEY_CONFIGREPLSET ='CONFIGREPLSET'
KEY_CPUSET        ='CPUSET'
KEY_CPUSETGEN     ='CPUSETGEN'

def validate_env():

    mandatoryParams = { KEY_BIND_IP,
                        KEY_CONFIGREPLSET,
                        KEY_CPUSETGEN }

    for key in mandatoryParams:
        if key not in os.environ:
            print "Missing mandatory argument:", key
            sys.exit(2)
    ok = False
    if KEY_CPUSET in os.environ:
        ok = True
    elif KEY_NODE_COUNT in os.environ:
        ok = True
    if KEY_CPUSET in os.environ and KEY_NODE_COUNT in os.environ:
        if int(os.environ[KEY_NODE_COUNT]) != len(os.environ[KEY_CPUSET].strip().split(',')):
            ok = False
    if not ok:
        print 'One of ', KEY_CPUSET, ', ', KEY_NODE_COUNT, 'must be provided. If both provided they must match'
        sys.exit(2)

def generate_yml():

    if KEY_NODE_COUNT in os.environ:
        totalNodes = int(os.environ[KEY_NODE_COUNT])
    elif KEY_CPUSET in os.environ:
        totalNodes = len(os.environ[KEY_CPUSET].strip().split(','))
    else:
        print 'Missing', KEY_NODE_COUNT, 'and', KEY_CPUSET
        exit(2)

    # below is optional, if provided we use it towards first node, and increment for each node.
    # if not provided, we just set port to zero assuming each node will do the right thing.
    if KEY_FIRST_TCP_PORT in os.environ:
        firstTCPPort = int(os.environ[KEY_FIRST_TCP_PORT])
    else:
        firstTCPPort = 0

    config = ''
    count = 0
    tcpPort = firstTCPPort
        
    try:
        with open('/tmp/node.config.yml', 'w') as f:
            config = 'nodes:\r\n'
            while count < totalNodes:
                config = config + \
                '  - name: \"' + str(count+1) + '\"\r\n' + \
                '    endpoint:\r\n'  + \
                '      type: IPv4\r\n' + \
                '      ip: ' + os.environ[KEY_BIND_IP] + '\r\n' + \
                '      port: ' + str(firstTCPPort) + '\r\n'
                if firstTCPPort > 0:
                    firstTCPPort = firstTCPPort + 1
                count = count + 1
            config = config + \
                'partitionManagerSet:\r\n'
            configReplSet = os.environ[KEY_CONFIGREPLSET].strip().split(',')
            for server in configReplSet:
                config = config + '  - address: \"' + server + '\"\r\n'
            if KEY_CPUSET in os.environ:
                config = config + \
                    'cpuset: \"' + os.environ[KEY_CPUSET] + '\"\r\n'
            config = config + \
                'cpuset_general: \"' + os.environ[KEY_CPUSETGEN] + '\"\r\n'
            f.write( config )
    except IOError as e:
        print("I/O error({errno}): {msg}".format(errno=e.errno, msg=e.strerror))
        sys.exit(2)
    except:
        print "Unexpected error:", sys.exc_info()[0]
        sys.exit(2)

def run():

    cmdline = '/usr/local/bin/node_pool --k2config {path}'.format( \
                   path='/tmp/node.config.yml')
    print cmdline
    status = os.system(cmdline)

    if os.WIFEXITED(status):
        if os.WEXITSTATUS(status) != 0:
            print 'node_pool failed.'
            sys.exit(2)

def main( argv ):

    validate_env()
    generate_yml()
    run()

if __name__ == "__main__":
    main( sys.argv[1:] )
