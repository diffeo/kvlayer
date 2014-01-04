set -x
sudo apt-get update
sudo apt-get install -y --force-yes git python-pip
sudo pip install gitpython
sudo stop salt-master
sudo stop salt-minion
sleep 5
pkill -9 -f '(salt-minion|salt-master)'
sudo start salt-master
sudo start salt-minion
sleep 30
sudo salt \* test.ping
sudo salt \* state.highstate
