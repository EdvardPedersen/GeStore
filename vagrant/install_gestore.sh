#!/usr/bin/env bash

# Install blast2 for the formatdb application needed by the uniprot plugin to create BLAST databases
sudo apt-get install blast2 -qq

# Download and build GeStore
git clone -q https://github.com/EdvardPedersen/GeStore.git
cd GeStore
mvn -q package

# Link GeStore to more accessible directories
mkdir ~/bin
ln target/gestore* ~/bin/gestore.jar
ln conf/gestore-conf.xml ~/gestore-conf.xml
ln src/scripts/gestore_get.py ~/bin/gestore_get
ln src/scripts/gestore_put.py ~/bin/gestore_put
cd ~

# Include GeStore in the classpaths
echo "export GESTORE_PATH=~/bin/gestore.jar" >> .bash_profile
echo "export HADOOP_CLASSPATH=/home/vagrant/bin/gestore.jar:`hbase classpath`:$HADOOP_CLASSPATH" >> .bash_profile
source .bash_profile
