#!/usr/bin/env bash

#Install zookeeper

sudo apt-get -qq install zookeeper-server

sudo mkdir -p /var/lib/zookeeper
sudo chown -R zookeeper /var/lib/zookeeper/

sudo service zookeeper-server init
sudo service zookeeper-server start
