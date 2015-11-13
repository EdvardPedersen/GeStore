#!/usr/bin/env bash

# Install Cloudera in Pseudo-distributed mode
sudo apt-get -qq update
sudo apt-get -qq install bigtop-utils -yq

sudo sh -c 'echo "export JAVA_HOME=/usr/lib/java" > /etc/default/bigtop-utils'
