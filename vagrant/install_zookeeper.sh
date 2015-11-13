#!/usr/bin/env bash

sudo apt-get -qq install zookeeper-server

sudo mkdir -p /var/lib/zookeeper
sudo chown -R zookeeper /var/lib/zookeeper/

sudo service zookeeper-server init
sudo service zookeeper-server start
#sudo /usr/lib/zookeeper/bin/zkServer-initialize.sh
#sudo sh /usr/lib/zookeeper/bin/zkServer.sh start
