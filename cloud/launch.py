#
# Script to launch Accumulo clusters.
#
# It uses the output of salt-cloud to determine the IP address of the master
# and connects to it to apply the accumulo-saltstack recipes.
#

import sys
import yaml
import os
from time import sleep

from fabric.api import sudo, execute, env, settings

saltconf_file = os.path.expanduser('~/.saltcloud-ec2.conf')

def launch_cluster(**kwargs):
    """Command to connect to the master to apply the states."""
    sudo("apt-get update")
    #Required pkgs to download recipes from git (salt gitfs).
    sudo("apt-get install -y --force-yes git python-pip")
    sudo("pip install gitpython")
    with settings(warn_only=True):
        sudo("stop salt-master")
        sudo("stop salt-minion")
        sleep(5)
        sudo("pkill -9 -f '(salt-minion|salt-master)'")
    sudo("start salt-master")
    sudo("start salt-minion")
    sleep(30)
    sudo("salt \* test.ping")
    sudo("salt \* state.highstate")

# Use the credentials from the current salt-cloud conf.
saltconf = yaml.load(open(saltconf_file))
keyfilename = saltconf["my-amz-credentials"]["private_key"]
env["key_filename"] = keyfilename
env["user"] = "ubuntu"

cloudmap = yaml.load(open('tmp/salt-cloud.out'))
master_ip = None
# The output is different for single node and multi-node clusters, try both.
try:
    for name, data in cloudmap['my-amz']['ec2'].iteritems():
        if 'master' in name:
             master_ip = data['public_ips']
except KeyError:
    for name, data in cloudmap.iteritems():
        if 'master' in name:
             master_ip = data['ipAddress']

if not master_ip:
    print 'FATAL: master IP not found'
    sys.exit(-1)

execute(launch_cluster, host=master_ip)

