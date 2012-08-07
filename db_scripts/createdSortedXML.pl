#!/usr/bin/perl -w

use strict;
use Data::Dumper;
use Getopt::Std;
use IO::File;

eval{
    _main();
};

if($@){
    print "ERRORS:\n";
    print STDOUT $@;
}

sub _main{
    our %opts = ();
    getopts("d:",\%opts);

    my $dir = $opts{'d'};
    _usage() unless (-d $dir);
    
    # Sorting by first two characters split into two because of maximum number
    # of filehandles opened at the same time 
    # sort the whole annotation file by first character

    # Sort by first charcter
    opendir(DIR,$dir) or die "Failed to open directory $dir.";
    my @files = grep{(-f "$dir/$_")&&($_=~/.*_annotations\.xml$/)}readdir(DIR);
    closedir(DIR);

    foreach my $file (@files){
	# create directory name for the sorted annotation files
	my $filePath = $dir."/$file";
	$filePath=~s/\/\//\//g;
	my $dirName = $dir."/$file";
	$dirName=~s/\.xml//;
	$dirName=~s/\/\//\//g;
	_parseFile($filePath,$dirName,1);
    }

    # Sort by two first characters
    opendir(DIR2,"$dir") or die "failed to open directory $dir.";
    my @dirs = grep {(-d "$dir/$_")&&($_!~/^[\s\t\n]*\.*[\s\t\n]*$/)}readdir(DIR2);
    closedir(DIR2);
    foreach my $oDirName (@dirs){
	my $oDir = "$dir/$oDirName";
	$oDir =~s/\/\//\//g;
	opendir(DIR3,$oDir) or die "Failed to open directory of ordered annotation files $oDir.";
	my @orderedFiles = grep{(-f "$oDir/$_")}readdir(DIR3);
	close(DIR3);
	foreach my $oFile (@orderedFiles){
	    my $oPath = $oDir."/$oFile";
	    $oPath=~s/\/\//\//g;
	    warn "\ndir1 = \'$dir\'\ndir2 = \'$dir\'\ndir3 = \'$oDir\'\nrm $oPath\n";
	    my @oFileSplit = split(/\./,$oFile);
	    my @oFilePrefix = split("",$oFileSplit[0]);
	    die "[ERROR] Trying to process ordered file $oFile" unless scalar(@oFilePrefix)==1;
	    _parseFile($oPath,$oDir,2);
	    system("rm $oPath");
	}
    }
    print STDOUT "\n\nDone!\n\n";
}


sub _parseFile{
    my ($file,$annotationDirName,$run) = @_;

    open(FILE,"<$file") or die "Failed to open annotation file $file for reading.";
   
    # in the first run the main annotation file is sorted by first characters of gene ids.
    # in the second run the first-character annotation files are read in again and 
    # sorted by second first character but written to the same directory.
    if($run==1){

	# create directory for first run 
	system("mkdir $annotationDirName") unless -d $annotationDirName;
	die "Failed to create directory $annotationDirName." unless -d $annotationDirName;
    }

    
    my ($first,$id);
    my $h = [];
    my $fileHandles = {};
    while(<FILE>){
        my $line = $_;
        next if $line=~/^\<Annotations\>[\t\s\n]*$/;
	next if $line=~/^\<\/Annotations\>[\t\s\n]*$/;
	push @$h,$line;
	
	if($line=~/^[\s\t]*\<\/Sequence\>[\s\t\n]*$/){
	    _printAnnotation($annotationDirName,$fileHandles,$first,$id,$h);
	    $id = "";
	    $h = [];
	    $first = "";
	}

	if($line=~/^[\s\t]*\<Sequence_ID\>(.*)\<\/Sequence_ID\>[\s\t\n]$/){
	    $id = $1;
	    my @idSplit = split("",$id);
		
	    # if run = 1 just take the first character
	    # if run = 2 take first two characters as key
	    if($run == 1){
		$first = $idSplit[0];
	    }
	    elsif($run == 2){
		$first = $idSplit[0].$idSplit[1];
	    }
	    else{
		die "[ERROR] Run variable in _parseFile neither 1 nor 2!";
	    }
	}
    }
    close(FILE);
    foreach my $key(keys(%$fileHandles)){
	my $fh = $fileHandles->{$key};
	print $fh "</Annotations>\n";
	close($fh);
    }
}



# print out the annotation.xml file
sub _printAnnotation{
    my ($annotationDirName,$fileHandles,$first,$id,$h) = @_;

#    warn "annodirname = $annotationDirName\nfirst = $first\nid= $id\n\n";

    # if there is no FH for first character open one
    if(!$fileHandles->{$first}){
        my $annotationFileName = $annotationDirName."/".$first.".xml";
	warn $annotationFileName;

	my $fh = IO::File->new(">$annotationFileName");
        print $fh "<Annotations>\n";

        $fileHandles->{$first} = $fh;
    }
    my $annofh = $fileHandles->{$first};
    foreach(@$h){
        print $annofh $_;
    }
}



sub _usage{
    print STDOUT "\n\nScript takes a non-sorted (old version) _annotation.xml file and creates one directory for each *.xml file found in a given directory.\n\n";
    print STDOUT "Parameter:\nd : directory the unsorted xml files are in\n\n";
    exit;
}

