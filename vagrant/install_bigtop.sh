#!/usr/bin/env bash

# Install bigtop to make sure we detect the correct java version
sudo apt-get -qq update
sudo apt-get -qq install bigtop-utils -yq

sudo sh -c 'echo "export JAVA_HOME=/usr/lib/java" > /etc/default/bigtop-utils'
