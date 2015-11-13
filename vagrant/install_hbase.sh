#!/usr/bin/env bash

sudo apt-get install -qq hbase -yq
sudo apt-get install -qq hbase-master -yq
sudo apt-get install -qq hbase-regionserver -yq

sudo sh -c 'sudo echo "export HBASE_CLASSPATH=/usr/share/java/zookeeper.jar" >> /etc/hbase/conf/hbase-env.sh'

sudo -u hdfs hadoop fs -mkdir /hbase
sudo -u hdfs hadoop fs -chown hbase /hbase

sudo sh -c 'sudo echo "<?xml version="1.0" encoding="UTF-8"?>" > /etc/hbase/conf/hbase-site.xml'
sudo sh -c 'sudo echo "<configuration>" >> /etc/hbase/conf/hbase-site.xml'
sudo sh -c 'sudo echo "  <property>" >> /etc/hbase/conf/hbase-site.xml'
sudo sh -c 'sudo echo "    <name>hbase.cluster.distributed</name>" >> /etc/hbase/conf/hbase-site.xml'
sudo sh -c 'sudo echo "    <value>true</value>" >> /etc/hbase/conf/hbase-site.xml'
sudo sh -c 'sudo echo "  </property>" >> /etc/hbase/conf/hbase-site.xml'
sudo sh -c 'sudo echo "  <property>" >> /etc/hbase/conf/hbase-site.xml'
sudo sh -c 'sudo echo "    <name>hbase.rootdir</name>" >> /etc/hbase/conf/hbase-site.xml'
sudo sh -c 'sudo echo "    <value>hdfs://myhost:8020/hbase</value>" >> /etc/hbase/conf/hbase-site.xml'
sudo sh -c 'sudo echo "  </property>" >> /etc/hbase/conf/hbase-site.xml'
sudo sh -c 'sudo echo "</configuration>" > /etc/hbase/conf/hbase-site.xml'

sudo service hbase-master start
sudo service hbase-regionserver start
