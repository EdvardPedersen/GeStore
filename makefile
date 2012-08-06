ENTRY_SOURCES = uniprotEntry.java genericEntry.java fastaEntry.java glimmerpredictEntry.java
SOURCE_SOURCES = sourceType.java uniprotSource.java sprot.java trembl.java fastaSource.java glimmerpredictSource.java
APPLICATION_SOURCE = move.java adddb.java getfasta.java dbutil.java getdeleted.java
INPUT_SOURCE = LongRecordReader.java DatInputFormat.java
EXPERIMENT_SOURCES = countupdates.java
DEPRECATED_SOURCE = cmpdb.java countupdates.java getdat.java getdeleted.java annotateBlastRes.java blastoutputformat.java combineBlastOutput.java
SOURCES = $(ENTRY_SOURCES) $(SOURCE_SOURCES) $(APPLICATION_SOURCE) $(INPUT_SOURCE) $(EXPERIMENT_SOURCES)
CLASSPATH_JAVA = /usr/lib/hadoop-0.20/hadoop-core.jar:/usr/lib/hbase/hbase-0.90.3-cdh3u1.jar:/usr/lib/hbase/hbase-0.90.1-cdh3u0.jar:/home/epe005/DiffDBMR/diffdb_classes/:/usr/lib/zookeeper/zookeeper.jar:.
JAR_PATH = /home/epe005/GeStoreGit/GeStore/
INPUT_DIR = /home/epe005/test_databases/
OUTPUT_DIR = /user/epe005/output/
SPROT_7_FILE = sprot_2011_07.dat
SPROT_8_FILE = sprot_2011_08.dat
TREMBL_7_FILE = trembl_2011_07.dat
TREMBL_8_FILE = trembl_2011_08.dat
REAL_RUN = `date +%s`

all:    $(SOURCES)
	rm -rf diffdb_classes/
	mkdir diffdb_classes
	javac -Xlint:unchecked -classpath $(CLASSPATH_JAVA) -d diffdb_classes $(SOURCES)
	jar -cvf diffdb.jar -C diffdb_classes/ .

update:
	#scp -r 129.242.19.56:/home/epe005/GeStore/DiffDBMR/* .
	#git pull
	make

test_all:
	make test_sprot
	make test_fasta
	make test_glimmer3

run_pipeline:
	#hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=500_pipeline_input -Dpath=$(INPUT_DIR)../sequences/masterBig/10mmaster.fas -Drun=500 -Dtimestamp_stop=$(REAL_RUN) -Dformat=fasta -Dtype=l2r -conf=$(JAR_PATH)gestore-conf.xml
	#hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=500_pipeline_input -Dtype=r2l -Drun=500 -conf=$(JAR_PATH)gestore-conf.xml
	#/opt/bio/glimmer/scripts/g3-iterated.csh 500_pipeline_input glimmer3.out
	#hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=500_glimmer_output -Dpath=glimmer3.out.predict -Drun=500 -Dtimestamp_stop=$(REAL_RUN) -Dformat=glimmerpredict -Dtype=l2r -conf=$(JAR_PATH)gestore-conf.xml
	hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=500_glimmer_output -Dtype=r2l -Drun=500 -conf=$(JAR_PATH)gestore-conf.xml
	mkdir protein
	mkdir nucleotide
	/opt/local/perl5122/bin/perl -I /home/epe005/workingGepan /home/epe005/workingGepan/GePan/scripts/exportFasta.pl -p "file=500_glimmer_output;script_id=500;" -c "GePan::Parser::Prediction::Cds::Glimmer3" -t "nucleotide,protein" -s 500_pipeline_input -o .

test_sprot:
	hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=sprot -Dpath=$(INPUT_DIR)sprot_201002.dat -Dtimestamp_stop=201002 -Dformat=uniprot -Dtype=l2r -conf=$(JAR_PATH)gestore-conf.xml
	hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=sprot -Dtimestamp_stop=201002 -Dtype=r2l -conf=$(JAR_PATH)gestore-conf.xml -Dregex=OC=.* -Dpath=testfile
	
test_fasta:
	hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=500_pipeline_input -Dpath=$(INPUT_DIR)../sequences/masterBig/10mmaster.fas -Drun=500 -Dtimestamp_stop=$(REAL_RUN) -Dformat=fasta -Dtype=l2r -conf=$(JAR_PATH)gestore-conf.xml
	hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=500_pipeline_input -Dtype=r2l -Drun=500 -conf=$(JAR_PATH)gestore-conf.xml -Dpath=testfile

test_glimmer3:
	/opt/bio/glimmer/scripts/g3-iterated.csh /home/epe005/sequences/masterBig/10mmaster.fas glimmer3.out
	hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=500_glimmer_output -Dpath=glimmer3.out.predict -Drun=500 -Dtimestamp_stop=$(REAL_RUN) -Dformat=glimmerpredict -Dtype=l2r -conf=$(JAR_PATH)gestore-conf.xml
	hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=500_glimmer_output -Dtype=r2l -Drun=500 -conf=$(JAR_PATH)gestore-conf.xml -Dpath=testfile

enter_sprot:
	#hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=sprot -Dpath=$(INPUT_DIR)sprot_201001.dat -Dtimestamp_stop=201001 -Dformat=uniprot -Dtype=l2r -conf=$(JAR_PATH)gestore-conf.xml
	hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=sprot -Dpath=$(INPUT_DIR)sprot_201002.dat -Dtimestamp_stop=201002 -Dformat=uniprot -Dtype=l2r -conf=$(JAR_PATH)gestore-conf.xml
	hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=sprot -Dpath=$(INPUT_DIR)sprot_201003.dat -Dtimestamp_stop=201003 -Dformat=uniprot -Dtype=l2r -conf=$(JAR_PATH)gestore-conf.xml

get_sprot:
	hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=sprot -Dtimestamp_stop=201002 -Dtype=r2l -conf=$(JAR_PATH)gestore-conf.xml -Dregex=OC=.*homo.*

enter_fasta:
	hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=500_pipeline_input -Dpath=$(INPUT_DIR)../sequences/masterBig/10mmaster.fas -Drun=$(REAL_RUN) -Dtimestamp_stop=$(REAL_RUN) -Dformat=fasta -Dtype=l2r -conf=$(JAR_PATH)gestore-conf.xml
	
get_fasta:
	hadoop jar $(JAR_PATH)diffdb.jar org.diffdb.move -Dfile=500_pipeline_input -Dtype=r2l -conf=$(JAR_PATH)gestore-conf.xml -Dpath=testfile

enter_07_sprot:
	hadoop jar diffdb.jar  org.diffdb.adddb $(INPUT_DIR)$(SPROT_7_FILE) sprot 201107 uniprot

enter_08_sprot:
	hadoop jar diffdb.jar  org.diffdb.adddb $(INPUT_DIR)sprot_2011_08.dat sprot 201108 uniprot

enter_09_sprot:
	hadoop jar diffdb.jar  org.diffdb.adddb $(INPUT_DIR)2011_09_sprot.dat sprot 201109 uniprot

diff_del_sprot:
	hadoop dfs -rmr $(OUTPUT_DIR)deleted
	hadoop jar diffdb.jar org.diffdb.getdeleted sprot $(OUTPUT_DIR)deleted 201109 uniprot

diff_sprot:
	#hadoop dfs -rmr $(OUTPUT_DIR)diffSprot
	hadoop jar diffdb.jar org.diffdb.countupdates gepan_sprot $(OUTPUT_DIR)diffSprot 201108 201109

enter_07_trembl:
	#hadoop dfs -copyFromLocal /home/epe005/GeStore/DiffDB/2011_07/trembl_2011_07.dat /epe005/input
	hadoop jar diffdb.jar org.diffdb.adddb $(INPUT_DIR)trembl_2011_07.dat trembl 201107 uniprot

enter_08_trembl:
	hadoop jar diffdb.jar org.diffdb.adddb $(INPUT_DIR)trembl_2011_08.dat trembl 201108 uniprot

enter_09_trembl:
	hadoop jar diffdb.jar org.diffdb.adddb $(INT_DIR)trembl_2011_09.dat trembl 201109 uniprot

compare_trembl:
	hadoop jar diffdb.jar org.diffdb.cmpdb 2011_07_trembl 2011_08_trembl trembl_diff

make_fasta_sprot_09:
	hadoop dfs -rmr $(OUTPUT_DIR)sprot.fasta
	hadoop jar diffdb.jar org.diffdb.getfasta -D input_table=sprot -D output_file=$(OUTPUT_DIR)sprot.fasta -D timestamp_start -D timestamp_stop=201109 -D regex=OC=.*BACterIa.* -D addendum=bacteria

make_fasta_sprot_08:
	hadoop dfs -rmr $(OUTPUT_DIR)sprot.fasta
	hadoop jar diffdb.jar org.diffdb.getfasta sprot $(OUTPUT_DIR)sprot.fasta 0 201108 bacteria

make_fasta_sprot_08_09:
	hadoop dfs -rmr $(OUTPUT_DIR)sprot.fasta
	hadoop jar diffdb.jar org.diffdb.getfasta sprot $(OUTPUT_DIR)sprot.fasta 201108 201109 bacteria

make_dat_sprot_07:
	hadoop dfs -rmr $(OUTPUT_DIR)sprot_07.dat
	hadoop jar diffdb.jar org.diffdb.getdat 2011_07_sprot $(OUTPUT_DIR)sprot_07.dat

copy_from_hdfs:
	hadoop jar diffdb.jar org.diffdb.move 1 r:/user/epe005/output/sprot.fasta/part-r-00000 l:/home/epe005/DiffDBMR/outputFileFromHDFS

add_trembl:
	#hadoop jar diffdb.jar org.diffdb.adddb $(INPUT_DIR)trembl_2011_07.dat trembl 201107 uniprot
	hadoop jar diffdb.jar org.diffdb.adddb $(INPUT_DIR)trembl_2011_08.dat trembl 201108 uniprot
	hadoop jar diffdb.jar org.diffdb.adddb $(INT_DIR)trembl_2011_09.dat trembl 201109 uniprot

add_sprot:
	hadoop jar diffdb.jar  org.diffdb.adddb $(INPUT_DIR)sprot_2011_08.dat sprot 201108 uniprot
	hadoop jar diffdb.jar  org.diffdb.adddb $(INPUT_DIR)2011_09_sprot.dat sprot 201109 uniprot
	

test_move:
	hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=sprot -D run=$(REAL_RUN) -D type=r2l -D timestamp_stop=201109 -D regex=OC=.*homo.*
	hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=sprot -D run=500 -D type=r2l -D timestamp_stop=201109 -D regex=OC=.*homo.*
	hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=testFile -D run=10 -D path=testFilel2r -D type=l2r
	hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=testFile -D run=10 -D path=testFiler2l -D type=r2l
	hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=testFile -D run=11 -D path=testFileWRONG -D type=r2l
	

test_full_sprot:
	hadoop jar diffdb.jar  org.diffdb.adddb $(INPUT_DIR)sprot_2011_08.dat sprot 201108 uniprot
	hadoop jar diffdb.jar  org.diffdb.adddb $(INPUT_DIR)2011_09_sprot.dat sprot 201109 uniprot
	hadoop dfs -rmr $(OUTPUT_DIR)deleted
	hadoop jar diffdb.jar org.diffdb.getdeleted sprot $(OUTPUT_DIR)deleted 201109 uniprot
	hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=sprot -D path=sprot_201108 -D type=r2l -D timestamp_stop=201109
	
master_test_1:
	(time hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=sprot -D type=r2l -D timestamp_stop=201112 -D timestamp_start=201112) 1> test1_out 2> test1_hadoop
	(time hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=trembl -D type=r2l -D timestamp_stop=201112 -D timestamp_start=201112) 1>> test1_out 2>> test1_hadoop

master_test_2:
	(time hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=sprot -D type=r2l -D timestamp_stop=201112 -D timestamp_start=201106) 1> test2_out 2> test2_hadoop
	(time hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=trembl -D type=r2l -D timestamp_stop=201112 -D timestamp_start=201106) 1>> test2_out 2>> test2_hadoop

master_test_3:
	(time hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=sprot -D type=r2l -D timestamp_stop=201112 -D timestamp_start=201101) 1> test3_out 2> test3_hadoop
	(time hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=trembl -D type=r2l -D timestamp_stop=201112 -D timestamp_start=201101) 1>> test3_out 2>> test3_hadoop
	
master_test_4:
	(time hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=sprot -D type=r2l -D timestamp_stop=201112) 1> test4_out 2> test4_hadoop
	(time hadoop jar /home/epe005/DiffDBMR/diffdb.jar org.diffdb.move -D file=trembl -D type=r2l -D timestamp_stop=201112) 1>> test4_out 2>> test4_hadoop
	
clean: 
	rm -rf diffdb_classes/
	mkdir diffdb_classes
