GeStore Tutorial
-------------------

This is the users guide and tutorial for using GeStore.

Using the preview VM
--------------------

The preview VM is found in the vagrant subdirectory of the git.
To use the VM, install vagrant, then do vagrant up in the directory of the vagrantfile, then do "vagrant ssh" to log in to the example box. This tutorial assumes you are using the preview VM.

Getting started
---------------

To start off with, we need to add some data. The preview VM automatically downloads three versions of the UniProt/Sprot database in fasta format, which we can use.

To add the Sprot database, do 

```
gestore_put test_data/201501/uniprot_sprot.fasta sprot --format=fasta --timestamp=201501
```

If you want to test that it was added correctly, do 

```
gestore_get sprot --run=1
```

The run parameter is used to identify a iteration of the pipeline (e.g. with the same tools and input data), to enable automatic incremental updates.

To do simple queries, you can also limit what is returned by using a regular expression, an example:

```
gestore_get sprot --regex=ID=.*sapiens.* --run=1 -f
```

The -f parameter tells GeStore to ignore previous runs of the pipeline, and get the full database, instead of getting an incremental version.

Let us continue by adding another couple of versions of the sprot meta-database.

```
gestore_put test_data/201502/uniprot_sprot.fasta sprot --format=fasta --timestamp=201502
gestore_put test_data/201503/uniprot_sprot.fasta sprot --format=fasta --timestamp=201503
```

We can now retrieve an update of the previous bacterial meta-database:

```
gestore_get sprot --regex=ID=.*sapiens.* --run=1
```


Configuration for your own cluster
----------------------------------

Config file.
Setting up HDFS and HBase.

The configuration can be found in ~/GeStore/conf/gestore.conf, this needs to be tailored to your own setup.

Configuration variable | Effect
-----------------------| ------
hdfs_base_path | The base path on HDFS where files are stored.
hdfs_temp_path | The path on HDFS where temporary files are stored
local_temp_path | The local temporary path
hbase_file_table | The name of the table in HBase where file information is stored
hbase_run_table | The name of the table in HBase where run information is stored (provenance information)
hbase_db_update_table | The name of the table where update information is stored for files

Advanced Usage
---------------

There are three interfaces you can use to add and retrieve data from GeStore. We already used the Python wrapper in the "Getting Started" section.

The Python interface is a wrapper to more easily generate the parameters for the Java application. The 

The full list of parameters for the Java interface:

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

You can also invoke GeStore through Hadoop, by doing "hadoop jar /path/to/GeStore.jar org.gestore.move"

In addition, GeStore can be accessed through an HDFS-like interface in org.gestore.fswrapper:

Method | Action
-------|-------
ls			|	List the files in a directory
getFile		|	Retrieves a file frm GeStore
getFiles	|	Retrieves all files in a directory
putFile		|	Put file into GeStore
putFiles	|	Put files into GeStore
mkdir		|	Dummy method (to fulfil interface requirements)
delFile		|	Delete a file
delDir		|	Delete a directory
getFileSize	|	Get the size of a file
lsRec		|	Do a recursive "ls"

Plugins
-------

New plugins can be added by extending the [genericEntry](src/main/java/org/gestore/plugin/entry/genericEntry.java) class for the entry part, and the [sourceType](src/main/java/org/gestore/plugin/source/sourceType.java) class for the source.

The source plugin will usually build a submission for the getfasta.main method (which runs a MapReduce job using the plugin specified). The most simple plugin will then rename and return the list of files produced by getfasta, a simple example of this is found in [fastaSource](src/main/java/org/gestore/plugin/source/fastaSource.java). It may also, in more complex plugins, do post-processing on the data retrieved by GeStore, such as in [uniprotSource](src/main/java/org/gestore/plugin/source/uniprotSource.java).

The entry plugin needs to implement six methods, in addition to the constructors. These methods are described in detail in [genericEntry](src/main/java/org/gestore/plugin/entry/genericEntry.java), and a fairly simple example is in the [fastaEntry](src/main/java/org/gestore/plugin/entry/fastaEntry.java) plugin. A quick summary in pseudocode of the methods that musta be implemented:

Method | Action
-------|--------
addEntry(entry) |  Adds an plaintext entry to the internal data structure used by the plugin, and formats it correctly internally.
getPartialPut(fields, timestamp) | Builds a Put containing all the columns specified in the fields vector, with the given timestamp.
sanityCheck(type) | Verifies that the internal data is complete enough to produce the given type (i.e. that no required fields are missing).
get(type, options) | Returns a representation of the entry in the given format and options
compare(entry) | Returns a list of updated or different elements
