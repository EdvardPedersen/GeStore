#!/usr/bin/env bash

#Update the system
sudo apt-get -qq update
sudo apt-get -qq upgrade

# Install dependencies
sudo apt-get -qq install curl maven git -yq

# Add the cloudera repo
wget http://archive.cloudera.com/cdh5/one-click-install/precise/amd64/cdh5-repository_1.0_all.deb -q
sudo dpkg -i cdh5-repository_1.0_all.deb
curl -s http://archive.cloudera.com/cdh5/ubuntu/precise/amd64/cdh/archive.key | sudo apt-key add -

# Prefer to use the cloudera repository
sudo printf "Package: *\nPin: origin archive.cloudera.com\nPin-Priority: 700" | sudo tee /etc/apt/preferences > /dev/null

# Install the JDK
# Hack to download the oracle JDK headless
wget --no-cookies --no-check-certificate --header "Cookie: oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/7u55-b13/jdk-7u55-linux-x64.tar.gz" -O jdk-7-linux-x64.tar.gz -q
sudo mkdir -p /usr/lib/jdk.1.7.0_55
sudo tar -zxf jdk-7-linux-x64.tar.gz -C /usr/lib/
echo "export JAVA_HOME=/usr/lib/java" > ~/.bash_profile
source ~/.bash_profile
sudo unlink /usr/lib/java
sudo ln -s /usr/lib/jdk1.7.0_55/ /usr/lib/java
