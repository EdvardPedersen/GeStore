GeStore Tutorial
-------------------

WORK_IN_PROGRESS

Using the preview VM
--------------------

The preview VM is found in the vagrant subdirectory of the git.
To use the VM, install vagrant, then do vagrant up in the directory of the vagrantfile, then do "vagrant ssh" to log in to the example box. This tutorial assumes you are using the preview VM.

Getting started
---------------

To start off with, we need to add some data. The preview VM automatically downloads three versions of the UniProt/Sprot database, which we can use.

To add the Sprot database, do "gestore_add sprot_2014_01.dat --name=sprot --format=uniprot --version=2014_01".

If you want to test that it was added correctly, do "gestore_get sprot".

Configuration for your own cluster
----------------------------------

Config file.
Setting up HDFS and HBase.

The configuration can be found in ~/GeStore/conf/gestore.conf, this needs to be tailored to your own setup.

Advanced Usage
---------------

Java interface. (Using the org.gestore.move class)
HDFS-like interface.
Parameters.

Parameter 	| Effect
----------	|-------
file		| Name of the data, which is used to access it through GeStore
type		| r2l (remote-to-local) or l2r (local-to-remote) to specify copying to and from GeStore
format		| Which plugin to use to parse the data
path		| Where the input file is
timestamp_start | Get data that has been updated since timestamp_start
timestamp_stop | Set the timestamp for the latest data to retrieve, the timestamp for the data when adding to GeStore
taxon		| Additional argument sent to the plugin
quick_add	| If true, return immidiately rather than wait for the operation to complete
split		| Create multiple files
copy		| If false, GeStore will return HDFS path(s) to the data
run			| The run ID, used to identify which runs can use incremental data
task		| Task ID, to identify multiple executions within the same run, on the same data
regex		| A regular expression on the form "FIELD=REGEX", to limit the results (e.g. -Dregex=TC=*.bacteria*.)
full_run	| If true, do not attempt to use incremental data

Configuration variable | Effect
-----------------------| ------
hdfs_base_path | The base path on HDFS where files are stored.
hdfs_temp_path | The path on HDFS where temporary files are stored
local_temp_path | The local temporary path
hbase_file_table | The name of the table in HBase where file information is stored
hbase_run_table | The name of the table in HBase where run information is stored (provenance information)
hbase_db_update_table | The name of the table where update information is stored for files
How to do things like queries, incremental databases.
How to get provenance information.

You can also invoke GeStore through Hadoop, by doing "hadoop jar /path/to/GeStore.jar org.gestore.move"

Plugins
-------
Adding new plugins
