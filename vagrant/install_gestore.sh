#!/usr/bin/env bash

git clone -q https://github.com/EdvardPedersen/GeStore.git
cd GeStore
mvn -q package
mkdir ~/bin
cp target/gestore* ~/bin/
cp conf/gestore-conf.xml ~/bin/
cd ~

echo "hadoop jar /home/vagrant/bin/gestore-0.1.jar org.gestore.move -Dpath=test_data/uniprot_sprot.dat -Dfile=sprot -Dtype=l2r -Dformat=uniprot -conf=/home/vagrant/bin/gestore-conf.xml" > run_gestore.sh
