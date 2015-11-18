#!/usr/bin/env bash

git clone -q https://github.com/EdvardPedersen/GeStore.git
cd GeStore
mvn -q package
mkdir ~/bin
ln target/gestore* ~/bin/gestore.jar
ln conf/gestore-conf.xml ~/gestore-conf.xml
ln src/scripts/gestore_get.py ~/bin/gestore_get
ln src/scripts/gestore_put.py ~/bin/gestore_put
cd ~

echo "export GESTORE_PATH=~/bin/gestore.jar" >> .bash_profile
echo "export HADOOP_CLASSPATH=/home/vagrant/bin/gestore.jar:`hbase classpath`:$HADOOP_CLASSPATH" >> .bash_profile
source .bash_profile

echo "hadoop jar /home/vagrant/bin/gestore-0.1.jar org.gestore.move -Dpath=test_data/uniprot_sprot.dat -Dfile=sprot -Dtype=l2r -Dformat=uniprot -conf=/home/vagrant/bin/gestore-conf.xml" > run_gestore.sh
