#!/usr/bin/perl

=head1 NAME

dbCron.pl

=head1 DESCRITPION

Script downloads and formats all needed/recommended databases that might be used in the annotation step of the GePan pipeline. The script should be run as a cron-job weekly or bi-weekly. 

Uses several tools that have to be installed on the system:

gunzip, formatdb, hmmpress

It creates two temporary directories: 
    
    GIVEN_DIR/tmp_uni: temp directory for uniprot database files

    GIVEN_DIR/tmp_pfam: temp directory for pfam database files

If needed output directories for formated databases and additional files are created:

    GIVEN_DIR/pfam: pfam database files and *.dat files as well as 'active_site.dat and sub-directories of berkeley database flat files for annotations.

    Given_DIR/uniprot: uniprot database files, *.list files (needed for FASTA run on blast databases) and sub-directories for berkeley database flat files for annotations



Creates dbCron.log log-file.

NOTE: Size of databases to be downloaded and processed >30GB. The resulting formated files and annotation xml files are smaller. But while processing the amount of memory needed can exceed 30GB.

=head1 PARAMETER

d : directory for database files

p : path to processDat.pl (part of GePan package, GePan/db_scripts)

=head1 STEPS

1. Check tools needed and create temporary download directoryies GIVEN_DIR/tmp_uni and GIVEN_DIR/tmp_pfam

2. Download database *.gz files to temporary pfam or uniprot directory 

3. Unzip Pfam files, format them using hmmpress and copying files to GIVEN_DIRECTORY/pfam

4. Calls processDat.pl to create berkely database flat files from Pfam-databases and copies them to GIVEN_DIRECTORY/pfam

5. Unzip uniprot files, run processDat.pl to create berkeley database flat files of all uniprot.dat files. Additionally it writes out a fasta file of all sequences in uniprot files.

6. Calls formatdb on created fasta files, creates *.list files for fasta35 searches on databases and moves all result files to GIVEN_DIRECTORY/uniprot

7. Delete temporary files and directories

=cut

use strict;
use Data::Dumper;
use Getopt::Std;

eval{
    _main();
};

if($@){
    print $@;
}

=head1 SUBS

=head2 _main

Checks for tool-paths, creates temporary directories and calls download and format methods.

=cut
sub _main{
    our %opts;
    getopts("d:p:s",\%opts);

    my $outDir = $opts{'d'};
    my $perlPath = $opts{'p'}; 
    my $perl = $perlPath?"$perlPath/processDat.pl":"./processDat.pl";
    my $silent = $opts{'s'}?$opts{'s'}:'0';

    if(!$outDir||(!(-e $perl))){
	_usage();
    }


    my $log = "dbCron.log";
    open(LOG,">$log") or dieLOG($silent,"Failed to open log file $log for writing.");

### Check directories and needed tools ###

    printLOG($silent,*LOG,"###### Check software paths and directory######### \n");

    # check if output directory exists
    dieLOG($silent,*LOG,"Output directory $outDir does not exist") unless (-d $outDir);

    # check path to formatdb
    my $formatdb = `which formatdb`;
    $formatdb=~s/\n//g;
    dieLOG($silent,*LOG,"No path to formatdb found") unless $formatdb;
    printLOG($silent,*LOG,"FORMATDB: \'$formatdb\'");

    # check path to hmmpress
    my $hmmpress = `which hmmpress`;
    $hmmpress=~s/\n//g;
    dieLOG($silent,*LOG,"No path to hmmpress found") unless $hmmpress;
    printLOG($silent,*LOG,"HMMPRESS: \'$hmmpress\'");

    # check path to gunzip
    my $gunzip = `which gunzip`;
    $gunzip=~s/\n//g;
    dieLOG($silent,*LOG,"No path to gunzip found") unless $gunzip;
    printLOG($silent,*LOG,"GUNZIP: \'$gunzip\'");

    # download databases to temporary download directories
    _downloadDBs($outDir,*LOG,$silent,$hmmpress,$formatdb,$gunzip,$perl);
}

=head2 _formatUni

Formating step for uniprot databases (sprot and trEmbl).

Calls gunzip on all *.gz files in GIVEN_DIR/tmp_uni. Calls:

1. processDat.pl to write out a fasta file of all database files and create a Berkeley database file of annotations.

2. formatdb with the created fasta files

Database files and directories are moved to GIVEN_DIR/uniprot. 

=cut 

# Uncompress files
# Call processUniprotDat.pl with *.dat files
# Delete *.dat files
# Call formatdb
# create *.list files 
# Move files and delete temporary directory 
sub _formatUni{
    my ($outDir,$formatdb,$gunzip,$perl,$log,$silent) = @_;

    printLOG($silent,$log,"\n##### Format uniprot db files #####");

    my $uni_dir = "$outDir/uniprot";
    $uni_dir=~s/\/\//\//g;

    my $uni_tmp = "$outDir/tmp_uni";
    $uni_tmp=~s/\/\//\//g;

    # create output directory if not existing
    if(!(-d $uni_dir)){
	printLOG ($silent,$log,"Create uniprot output directory $uni_tmp");
	my $exit = system("mkdir $uni_dir");
	dieLOG ($log,"Failed to create output directory $uni_dir") if $exit;
    }

    # unzip *.gz files
    opendir(DIR,$uni_tmp) or die "Failed to open directory $uni_tmp";
    my @gzs = grep {$_=~/^.*\.gz$/;} readdir(DIR);
    closedir(DIR);

    foreach(@gzs){
        my $file = "$uni_tmp/$_";
        dieLOG($silent,$log,"Failed to unzip file $file: no such file") unless -e $file;
        printLOG($silent,$log,"Unzipping file $file");
        my $exit = system("gunzip $file");
        dieLOG($silent,$log,"Failed to unzip file $file") if $exit;	
    }


    # Call processDat.pl for each *.dat file
    opendir(DIR,$uni_tmp) or die "Failed to open directory $uni_tmp";
    my @dats = grep {$_=~/^.*\.dat$/;} readdir(DIR);
    closedir(DIR);
    
    foreach(@dats){
	my $file = "$uni_tmp/$_";
	next unless (-f $file);
	dieLOG($silent,$log,"File $file does not exist.") unless -e $file;
	printLOG($silent,$log,"Calling $perl -f $file -d $uni_tmp");
	my $exit = system("perl $perl -f $file -d $uni_tmp -t uniprot");
	dieLOG($silent,$log,"Calling $perl failed") if $exit;
	printLOG($silent,$log,"Removing file $file");
	$exit = system("rm $file");
	dieLOG($silent,$log,"Failed to remove file $file") if $exit;
    }     

    # Get and format *.fas files
    opendir(DIR,$uni_tmp) or die "Failed to open directory $uni_tmp.";
    my @fastas = grep {$_=~/^.*\.fas$/;} readdir(DIR);
    closedir(DIR);

    foreach(@fastas){
	my $file = "$uni_tmp/$_";
	my $tmp = $_;
	dieLOG($silent,$log,"File $file does not exist") unless -e $file;
	printLOG($silent,$log,"Calling formatdb:\n$formatdb -i $file -p T -t $tmp");
	my $exit = system("$formatdb -i $file -p T -t $tmp");
	dieLOG($silent,$log,"Failed to format file $file") if $exit;

	# create list file
	printLOG($silent,$log,"Checking for multiple database files");
	open(LIST,">$uni_dir/$tmp.list") or die "Failed to open list file $uni_dir/$tmp.list for writing";
	print LIST "<$uni_dir\n";

	opendir(DIR,$uni_tmp) or dieLOG($silent,$log,"Failed to open file $uni_tmp for reading.");

	# check for more than one *.pin file
	my @frags = grep {$_=~/$tmp\.\d+\.pin$/;}readdir(DIR);
	closedir(DIR);
	if(scalar(@frags)){
	    # write *.list file for FASTA program
	    printLOG($silent,$log,"Writing $uni_dir/$file.list file");
	    foreach(@frags){
		my @frag = split(/\.pin/,$_);
		dieLOG($silent,$log,"Wrong number in split") unless scalar(@frag)==1;
		print LIST $frag[0]." 12\n";
	    }
	    
	    # formatdb created *.pal file containing paths to DB-parts.
	    # '../tmp_uni/..' has to be changed to '../uniprot/..'
	    open(PAL,"<$file.pal") or dieLOG($silent,$log,"Failed to open pal-file $file.pal for reading.");
	    printLOG($silent,$log,"Writing out temporary *.pal file.");
	    open(TMPPAL,">$file.pal.tmp") or dieLOG($silent,$log,"Failed to open temporary pal-file $file.pal.tmp for writing.");
	    while(<PAL>){
		if($_=~/^#.*$/){
		    print TMPPAL $_;
		}
		else{
		    my $line = $_;
		    $line=~s/tmp_uni/uniprot/g;
		    print TMPPAL $line;
		}
	    }
	    close(PAL);	
	    close(TMPPAL);
	    # removing old pal-file and rename new one
	    printLOG($silent,$log,"Removing old pal-file $file.pal");
	    my $e = system("rm $file.pal");
	    dieLOG($silent,$log,"Failed to remove pal-file $file.pal.") if ($e);
	    printLOG($silent,$log,"Moving temporary pal-file.");
	    $e = system("mv $file.pal.tmp $file.pal");
	    dieLOG($silent,$log,"Failed to move temporary pal-file $file.pal.tmp to $file.pal") if ($e);
	}
	else{
	    print LIST $tmp." 12\n";
	}
	close(LIST);
	
	# deleting fasta file
	printLOG($silent,$log,"Removing file $file");
	$exit = system("rm $file");
	dieLOG($silent,$log,"Failed to delete $file") if $exit;

	# Move database files and annotation database directory to $uni_dir
	opendir(DIR,$uni_tmp);
	my @content = readdir(DIR);
	closedir(DIR);
	foreach my $f (@content){
	    my $fPath = "$uni_tmp/$f";
	    dieLOG($silent,$log,"File $fPath does not exist.") unless ((-f $fPath)||(-d $fPath));
	    if((-d $fPath)&&($f!~/^.*_annotation.*$/)){
		next;
	    }
	    printLOG($silent,$log,"* Moving $fPath to directory $uni_dir");
	    $exit = system("mv $fPath $uni_dir");
	    dieLOG($silent,$log,"Failed to move $fPath to directory $uni_dir") if ($exit);
	}
    }

    # remove temporary directory
    printLOG($silent,$log,"Removing $uni_tmp");
    my $exit = system("rmdir $uni_tmp");
    dieLOG($silent,$log,"Failed to delet $uni_tmp") if $exit;
}

=head2 _formatPfam

Calls gunzip on all *.gz files in GIVEN_DIR/tmp_pfam. Then calls processDat.pl with unzipped files. Hmmpress is called on *.hmm files. Resulting database files (and active_site.dat) are moved to GIVEN_DIR/pfam and temporary directory is deleted.

=cut

# Uncompresses files
# Calls hmmpress to format pfam databases
# Extracts sequence description from and creates *.dat files
# Copies formated files and related *.dat files to pfam directory
# Deletes temporary files and directories
sub _formatPfam{
    my ($outDir,$hmmpress,$gunzip,$log,$silent,$perl) = @_;

    printLOG($silent,$log,"\n##### Format Pfam dbs #####\n");

    my $pfam_dir = $outDir."/pfam";
    $pfam_dir=~s/\/\//\//g;

    my $pfam_tmp = $outDir."/tmp_pfam";
    $pfam_tmp=~s/\/\//\//g;

    # create output directory if not existing
    if(!(-d $pfam_dir)){
	printLOG($silent,$log,"Create Pfam output directory $pfam_dir");
	my $exit = system("mkdir $pfam_dir");
	dieLOG($silent,$log,"Failed to create output directory $pfam_dir") if $exit;
    }

    # gunzip all *.gs files in pfam_tmp directory
    opendir(DIR,$pfam_tmp) or die "Failed to open directory $pfam_tmp for reading";
    my @gzs = grep {$_=~/^.*\.gz$/;}readdir(DIR);
    closedir(DIR);

    foreach(@gzs){
	my $file = "$pfam_tmp/$_";
	dieLOG($silent,$log,"Failed to unzip file $file: no such file") unless -e $file;
	printLOG($silent,$log,"Unzipping file $file");
	my $exit = system("gunzip $file");
	dieLOG($silent,$log,"Failed to unzip file $file") if $exit;
    }
    
    #### Process Pfam-A.full
    # call processDat.pl to create database flat files of important information from complete Stockholm file
    printLOG($silent,$log,"Calling processDat for Pfam-A\nstatement = perl $perl -f $pfam_tmp/Pfam-A.full -d $pfam_dir -t pfam");
    my $e = system("perl $perl -f $pfam_tmp/Pfam-A.full -d $pfam_tmp -t pfam");
    dieLOG($silent,$log,"Failed to execute $perl") if ($e);

    # remove Pfam-A.full
    printLOG($silent,$log,"Removing file $pfam_tmp/Pfam-A.full");
    $e = system("rm $pfam_tmp/Pfam-A.full");
    dieLOG($silent,$log,"Failed to remove file $pfam_tmp/Pfam-A.full") if ($e);

    #### Process Pfam-B
    # call processDat.pl to create database flat files of important information from complete Stockholm file
    printLOG($silent,$log,"Calling processDat for Pfam-B");
    $e = system("perl $perl -f $pfam_tmp/Pfam-B -d $pfam_tmp -t pfam");
    dieLOG($silent,$log,"Failed to execute $perl") if ($e);

    # remove Pfam-B.full
    printLOG($silent,$log,"Removing file $pfam_tmp/Pfam-B");
    $e = system("rm $pfam_tmp/Pfam-B");
    dieLOG($silent,$log,"Failed to remove file $pfam_tmp/Pfam-B") if ($e);

    # Moving annotation directories to pfam_dir
    opendir(DIR,$pfam_tmp);
    my @dirs = grep{(-d "$pfam_tmp/$_")&&($_=~/^.*annotation.*/)}readdir(DIR);
    closedir(DIR);
    dieLOG($silent,$log,"No directories of annotation databases found in $pfam_tmp.") unless scalar(@dirs);
    foreach(@dirs){
	my $dirName = "$pfam_tmp/$_";
	$e = system("mv $dirName $pfam_dir");
	dieLOG($silent,$log,"Failed to move directory $dirName to $pfam_dir.") if ($e);
    }

    # format database files
    # using hmmpress
    printLOG($silent,$log,"Formating Pfam-A:\nStatement = $hmmpress $pfam_tmp/Pfam-A.hmm");
    $e = system("$hmmpress $pfam_tmp/Pfam-A.hmm");
    dieLOG($silent,$log,"Failed to format Pfam-A.hmm") if $e;

    printLOG($silent,$log,"Formating Pfam-B:\nStatement = $hmmpress $pfam_tmp/Pfam-B.hmm");
    $e = system("$hmmpress $pfam_tmp/Pfam-B.hmm");
    dieLOG($silent,$log,"Failed to format Pfam-B.hmm") if $e;

    # delete *.hmm files
    printLOG($silent,$log,"Removing *.hmm files");
    $e = system("rm $pfam_tmp/Pfam-A.hmm");
    dieLOG($silent,$log,"Failed to delete $pfam_tmp/Pfam-A.hmm") if $e;
    $e = system("rm $pfam_tmp/Pfam-B.hmm");
    dieLOG($silent,$log,"Failed to delete $pfam_tmp/Pfam-B.hmm") if $e;

    # move formated files to output directory
    printLOG($silent,$log,"Moving formated files to $pfam_dir");
    $e = system("mv $pfam_tmp/* $pfam_dir");
    dieLOG($silent,$log,"Failed to move $pfam_tmp/* $pfam_dir") if $e;

    # remove temporary pfam directory
    printLOG($silent,$log,"Removing $pfam_tmp");
    $e = system("rmdir $pfam_tmp");
    dieLOG($silent,$log,"Failed to remove $pfam_tmp") if $e;
}


=head2 _downloadDBs

Downloads Pfam and uniprot database *.gz files to temporary directories. 

Pfam files downloaded from 

ftp://ftp.sanger.ac.uk/pub/databases/Pfam/current_release/

Uniprot database files (taxonomic divisions) are downloaded from

ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/

(For detailed list of databases see 'List of databases')

=cut

### Download database files
sub _downloadDBs{
    my ($outDir,$log,$silent,$hmmpress,$formatdb,$gunzip,$perl) = @_;
   
    printLOG($silent,$log,"\n##### Download db files #####\n");
    # create temporary download directory for uniprot files
    my $uni_tmp = "$outDir/tmp_uni/";
    $uni_tmp=~s/\/\//\//g;

    printLOG($silent,$log,"Creating temporary uniprot directory");
    system("mkdir $uni_tmp");
    dieLOG($silent,$log,"Failed to create temporary directory for uniprot database downloads.") unless (-d $uni_tmp);

    # create temporary download directory for pfam files
    my $pfam_tmp = "$outDir/tmp_pfam/";
    $pfam_tmp=~s/\/\//\//g;

    printLOG($silent,$log,"Creating temporary pfam directory");
    system("mkdir $pfam_tmp");
    dieLOG($silent,$log,"Failed to create temporary directory for pfam database downloads.") unless (-d $pfam_tmp);

    # download Pfam database
    # ftp site for Pfam files
    my $pfamURI = "ftp://ftp.sanger.ac.uk/pub/databases/Pfam/current_release";
    printLOG($silent,$log,"Download Pfam database files to $pfam_tmp");
    my $statement = "wget -P $pfam_tmp $pfamURI/Pfam-A.hmm.gz";
    my $exit = system($statement);
    dieLOG($silent,$log,"Failed to download Pfam-A.hmm.gz") if $exit;

    $statement = "wget -P $pfam_tmp $pfamURI/Pfam-A.hmm.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download Pfam-A.hmm.dat.gz") if $exit;
    $statement = "wget -P $pfam_tmp $pfamURI/Pfam-B.hmm.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download Pfam-B.hmm.gz") if $exit;

    $statement = "wget -P $pfam_tmp $pfamURI/Pfam-B.hmm.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download Pfam-B.hmm.dat.gz") if $exit;
    $statement = "wget -P $pfam_tmp $pfamURI/active_site.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download active_site.dat.gz") if $exit;

    $statement = "wget -P $pfam_tmp $pfamURI/Pfam-A.full.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download Pfam-A.full.gz.") if $exit;
    $statement = "wget -P $pfam_tmp $pfamURI/Pfam-B.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download Pfam-B.gz") if $exit;

    _formatPfam($outDir,$hmmpress,$gunzip,$log,$silent,$perl);

    # download Uniprot database
    # ftp site for uniprot database files
    my $uniprotURI = "ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions";
    printLOG($silent,$log,"Download uniprot taxonomic divisions to $uni_tmp");

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_sprot_archaea.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_sprot_archaea.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_sprot_bacteria.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_sprot_bacteria.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);
    
    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_sprot_fungi.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_sprot_fungi.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_sprot_invertebrates.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_sprot_invertebrates.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_sprot_mammals.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_sprot_mammals.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_sprot_plants.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_sprot_plants.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_sprot_rodents.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_sprot_rodents.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_sprot_vertebrates.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_sprot_vertebrates.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_sprot_viruses.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_sprot_viruses.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_trembl_archaea.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_trembl_archaea.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_trembl_bacteria.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_trembl_bacteria.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_trembl_fungi.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_trembl_fungi.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_trembl_invertebrates.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_trembl_invertebrates.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_trembl_mammals.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_trembl_mammals.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_trembl_plants.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_trembl_plants.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_trembl_rodents.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_trembl_rodents.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_trembl_unclassified.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_trembl_unclassified.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_trembl_vertebrates.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_trembl_vertebrates.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

    $statement = "wget -P $uni_tmp $uniprotURI/uniprot_trembl_viruses.dat.gz";
    $exit = system($statement);
    dieLOG($silent,$log,"Failed to download uniprot_trembl_viruses.dat.gz") if $exit;
    _formatUni($outDir,$formatdb,$gunzip,$perl,$log,$silent);

}


=head2 printLOG

For printing status messages to log-file

=cut

# prints log-message to log
sub printLOG{
    my ($silent,$log,$message) = @_;
    warn $message unless $silent;
    print $log $message."\n";
}

=head2 dieLOG

For unexpected errors print message to log and die

=cut

# Print die-message to log and exit;
sub dieLOG{
    my ($silent,$log,$message) = @_;
    print $log "[ERROR] $message]\n";
    close($log);
    die $message;
}


=head1 LIST OF DATABASES

The databases and depending files to  download are:

B<Pfam:>

Pfam-A.hmm.gz

Pfam-A.hmm.dat.gz

Pfam-B.hmm.gz

Pfam-B.hmm.dat.gz

active_site.dat.gz

B<Uniprot taxonomic divisions:>

uniprot_sprot_archaea.dat.gz

uniprot_sprot_bacteria.dat.gz

uniprot_sprot_fungi.dat.gz

uniprot_sprot_invertebrates.dat.gz

uniprot_sprot_mammals.dat.gz

uniprot_sprot_plants.dat.gz

uniprot_sprot_rodents.dat.gz

uniprot_sprot_vertebrates.dat.gz

uniprot_sprot_viruses.dat.gz

uniprot_trembl_archaea.dat.gz

uniprot_trembl_bacteria.dat.gz

uniprot_trembl_fungi.dat.gz

uniprot_trembl_invertebrates.dat.gz

uniprot_trembl_mammals.dat.gz

uniprot_trembl_plants.dat.gz

uniprot_trembl_rodents.dat.gz

uniprot_trembl_unclassified.dat.gz

uniprot_trembl_vertebrates.dat.gz

uniprot_trembl_viruses.dat.gz

=cut


sub _usage{
    print STDOUT "\nCron-job script that downloads uniprot and Pfam databases and formats it.\n";
    print STDOUT "Uniprot databases are downloaded as *.dat.gz files separated in taxa and preprocesses them before calling formatdb.\n";
    print STDOUT "Additionally *.list files for blast databases > 2GB are created to enable fasta searches.\n";
    print STDOUT "Uniprot databases are saved to GIVEN_DIRECTORY/uniprot/\n";
    print STDOUT "Pfam databases are stored to GIVEN_DIRECTORY/pfam/\n";
    print STDOUT "\nParameter:\n";
    print STDOUT "d : database directory.\n";
    print STDOUT "p : path to perl script processUniprotDat.pl. Default is \'./\' (optional)\n";
    print STDOUT "s : output just written to log-file, no STDOUT output (optional).\n";
    print STDOUT "\n";
    exit;
}
