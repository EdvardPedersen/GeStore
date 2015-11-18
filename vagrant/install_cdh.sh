#!/usr/bin/env bash



echo "export HADOOP_CLASSPATH=/usr/share/java/zookeeper.jar:$HADOOP_CLASSPATH" >> /home/vagrant/.bash_profile
echo "export PATH=~/bin/:$PATH" >> /home/vagrant/.bash_profile
source /home/vagrant/.bash_profile

sudo apt-get install hadoop-conf-pseudo -y

# Format HDFS
sudo -u hdfs hdfs namenode -format

# Start HDFS
for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do sudo service $x start ; done

sudo -u hdfs hdfs dfs -rm -r /tmp
sudo -u hdfs hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done_intermediate
sudo -u hdfs hdfs dfs -chown -R mapred:mapred /tmp/hadoop-yarn/staging
sudo -u hdfs hdfs dfs -chmod -R 1777 /tmp
sudo -u hdfs hdfs dfs -mkdir -p /var/log/hadoop-yarn
sudo -u hdfs hdfs dfs -chown yarn:mapred /var/log/hadoop-yarn

sudo service hadoop-yarn-resourcemanager start
sudo service hadoop-yarn-nodemanager start
sudo service hadoop-mapreduce-historyserver start

sudo -u hdfs hdfs dfs -mkdir -p /user/vagrant
sudo -u hdfs hdfs dfs -chown vagrant:supergroup /user/vagrant
