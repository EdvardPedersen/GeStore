#!/usr/bin/perl -w

=head1 NAME

processDat.pl

=head1 DESCRIPTION

Script takes either a uniprot.dat or a Pfam-HMM.dat file, parses out important information and creates Berkeley database flat files from it.

Additionally, if a uniprot file is given, a fasta file of all sequences is written out.

B<Fasta Sequence Names>.

    Fasta file is named like the *.dat just with ending ".fas"

    Additionally each gene is named : GENE_ID database_name 

B<Berkeley database flat files>

    A directory for the resulting annotations.dat files is created.

=head1 PARAMETER

f : database.dat file to process

d : output directory

t: type of *.dat file (either\'pfam\' or \'uniprot\')

h : show help (optional)

=cut

use strict;
use Getopt::Std;
use Data::Dumper;
use DB_File; 
use Tie::File;
eval{
    _main();
};

if($@){
    print "\n Errors \n";
    print $@;
}


sub _main{
    our %opts;
   getopts("t:f:d:h",\%opts);
    my $file = $opts{'f'};
    my $dir = $opts{'d'};
    my $help = $opts{'h'};
    my $type = $opts{'t'};

    if((!$file)||(!$dir)||($help)){
	_usage();
    }

    my @split = split(/\//,$file);
    my $taxon;
    my $tmp = pop(@split);
    if($tmp=~/^(.*)\.dat$/){
	$taxon = $1;
    }
    elsif($tmp=~/^(.*)\.full$/){
	$taxon = $1;
    }
    else{
	$taxon = $tmp;
    }

    my $annotationDirName = $dir."/".$taxon."_annotations";
    $annotationDirName=~s/\/\//\//g;

    warn "** Creating sub-directory $annotationDirName for annotation flat files.";
    my $exit = system("mkdir $annotationDirName");
    die "Failed to create annotation sub-directory $annotationDirName" if $exit;

    my $fastaName = $dir."/".$taxon.".fas ";
    $fastaName=~s/\/\//\//g;

    if($type eq 'pfam'){
	warn "* Processing file $file...";
	_processPfam($file,$annotationDirName);
    }
    elsif($type eq 'uniprot'){
	_processUniprot($file,$annotationDirName,$fastaName,$taxon);
    }
    else{
	_usage();
    }
}

# Prints annotation hash to database 
sub _printAnnotation{
    my ($annotationDirName,$values) = @_;
    # create new Berkeley_DB 
    my %index;

    tie %index, 'DB_File', "$annotationDirName/index.dat" or die "Can't initialize database: $!\n";
    foreach my $id (keys(%$values)){
	my $value = $values->{$id};
	$index{$id} = $value;
    }
    untie %index;
}


# Parses a Pfam.dat file and creates database flat file of it.
sub _processPfam{
    my ($file,$annotationDirName) = @_;

    open(FILE,"<$file") or die "Failed to open file $file for reading.";

    my $annotationFile = "$annotationDirName/annotations.dump";
    open(ANNO,">$annotationFile") or die "Failed to open annotation file $annotationFile for writing.";

    my $a = {};
    my $dbHash = {};
    my $id;
    my $datasetCount = 0;
    while(<FILE>){
        my $line = $_;
        if($line=~/^\/\/$/){
            if($id){
                $a->{'id'} = $id;
		$a->{'type'} = "pfam";
                $a->{'functional'} = 1;
		my $dump = Data::Dumper->new([$a],[qw($annotation)])->Purity(1)->Dump();
		my $pointer = tell(ANNO);
		my $length = length($dump);
		print ANNO $dump;
		my $value = "$pointer||$length";
		if(scalar(keys(%$dbHash))==100000){
		    $datasetCount+=100000;
		    print STDOUT "\nDatasets written to database (total): $datasetCount\n";
		    $dbHash->{$id} = $value;
		    _printAnnotation($annotationDirName,$dbHash);
		    $dbHash = {};
		}
		else{
		    $dbHash->{$id} = $value;
		}

                $a = {};
                $id = "";
            }
        }
        if($line=~/^#=GF\sID(.*)$/){
            $id = $1;
            $id=~s/ //g;
        }
        elsif($line=~/^#=GF\sAC(.*)$/){
            my $accession = $1;
            $accession=~s/ //g;
            $a->{'accession'} = $accession;
        }
        elsif($line=~/^#=GF\sDE[\s]+(.*)$/){
            my $desc = $1;
            $a->{'description'} = $desc;
        }
        elsif($line=~/^#=GF\sTP(.*)$/){
            my $type = $1;
            $type=~s/ //g;
            $a->{'type'} = $type;
        }
        elsif($line=~/^#=GF\sGA(.*)$/){
            my $ga = $1;
            $ga=~s/;//g;
            $a->{'gathered_threshold'} = $ga;
        }
        elsif($line=~/^#=GF\sDC(.*)$/){
            my $db_comment = $1;
            $a->{'db_comment'}=$a->{'db_comment'}?$a->{'db_comment'}.$db_comment:$db_comment;
        }
	elsif($line=~/^#=GF\sTC(.*)$/){
            my $trusted_cutoff = $1;
            $trusted_cutoff=~s/;//g;
            $a->{'trusted_cutoff'} = $trusted_cutoff;
        }
        elsif($line=~/^#=GF\sNC(.*)$/){
            my $nc = $1;
            $nc=~s/;//g;
            $a->{'noise_cutoff'} = $nc;
        }
        elsif($line=~/^#=GF\sCC(.*)$/){
            my $comment = $1;
            $a->{'comment'} = $a->{'comment'}?$a->{'comment'}.$comment:$comment;
        }
        elsif($line=~/^#=GF\SDR\s+PFAMA;(.*)\.*\d*[\s\t\n]*$/){
            my $ref = $1;
            $a->{'db_ref'} = $a->{'db_ref'}?$a->{'db_ref'}.$ref:$ref;
        }
    }
    close(ANNO);
    close(FILE);
    _printAnnotation($annotationDirName,$dbHash);
}


# print sequence fasta file
sub _printFasta{
    my ($fastafh,$id,$seq,$taxon) = @_;    
    print $fastafh ">$id $taxon\n$seq\n"; 
}

# read uniprot.dat file
sub _processUniprot{
    my ($file,$annotationDirName,$fastaFileName,$taxon) = @_;

    open(FASTA,">$fastaFileName") or die "Failed to open fasta file $fastaFileName for writing.";
    open(FILE,"<$file") or die "Failed to open file $file for reading.";

    my $annotationFile = "$annotationDirName/annotations.dump";
    open(ANNO,">$annotationFile") or die "[ERROR] Failed to open annotation file $annotationFile for writing.";
    my ($id,$seq,$tax,$first);
    my $h = {};
    my $datasetCount = 0;
    my $values = {};
    while(<FILE>){
        my $line = $_;
        # Get the sequence ID
        if($line=~/^ID[\s\t]+([\da-zA-Z_\-\.]+)[\s\t]*.*$/){
            if($id){
                foreach(keys(%$h)){
                    if($h->{$_}=~/^\s+(.*)[\.\n]+$/){
                        $h->{$_} = $1;
                    }
                }
                my $s = $h->{'sequence'};
                $s=~s/\t//g;
                $s=~s/\s//g;
                $h->{'sequence'} = $s;

                die "No sequence found for $id." unless $h->{'sequence'};
		$h->{'id'} = $id;
		$h->{'type'}="uniprot";

                # Create directory for sorted annotation files if needed
                if(!-d $annotationDirName){
                    system("mkdir $annotationDirName");
                }
                _printFasta(*FASTA,$id,$h->{'sequence'},$taxon);

		my $dump = Data::Dumper->new([$h],[qw($annotation)])->Purity(1)->Dump();
                my $pointer = tell(ANNO);
                my $length = length($dump);
                print ANNO $dump;
                my $value = "$pointer||$length";
		if(scalar(keys(%$values))==100000){
		    $datasetCount+=100000;
		    print STDOUT "\nDatasets written to database (total): $datasetCount\n";
		    $values->{$id} = $value;
		    _printAnnotation($annotationDirName,$values);
		    $values = {};
		}
		else{
		    $values->{$id} = $value;
		}
	    }
            $id = $1;
            $h = {};
            $seq = 0;
        }

	# Get full recName/annotation
	if($line=~/^DE[\s\t]+RecName\:(.*)\;\n$/){
	    my $value = $1;
	    if($value=~/^[\s\t]*\w+=(.*)$/){
		$value = $1;
	    }
	    if(exists($h->{'annotation'})){
		$h->{'annotation'}.="\t$value";
	    }
	    else{
		$h->{'annotation'} = $value;
	    }
	}
	# Add subName to annotation
	if($line=~/^DE[\s\t]+SubName\:(.*)\;\n$/){
            my $value = $1;
            if($value=~/^[\s\t]*\w+=(.*)$/){
                $value = $1;
            }
            if(exists($h->{'annotation'})){
                $h->{'annotation'}.="\t$value";
            }
            else{
                $h->{'annotation'} = $value;
            }
        }
	# Add AltName to annotation
	if($line=~/^DE[\s\t]+AltName\:(.*)\;\n$/){
            my $value = $1;
            if($value=~/^[\s\t]*[_\w]+=(.*)$/){
                $value = $1;
            }
            if(exists($h->{'annotation'})){
                $h->{'annotation'}.="\t$value";
            }
            else{
                $h->{'annotation'} = $value;
            }
        }
	# Get organism 
	if($line=~/^OS[\s\t]+(.*)\.$/){
	    $h->{'organism'} = $1;
	}
	# Get NCBI_taxonomy
	if($line=~/^OX[\s\t]*NCBI_TaxID=(\d+)\;\n$/){
	    $h->{'taxonomy_id'} = $1;
	}
	# Get EMBL_ID
	if($line=~/^DR[\s\t]+(EMBL.*)\n$/){
	    my @split = split(/;/,$line);
	    die "Wrong number of elements in EMBL-tag." unless scalar(@split)==5;
	    $h->{'embl'} = $split[1]." ".$split[2];
	}
	# Get PIR-ID
	if($line=~/^DR[\s\t]+(PIR;.*)\n$/){
	    my @split = split(/\;/,$line);
	    print Dumper @split unless scalar(@split)==3;
	    die "Wrong number of elements in PIR-tag." unless scalar(@split)==3;
	    $h->{'pir'} = $split[1]." ".$split[2];
	}
	# Get pfam 
	if($line=~/^DR[\s\t]+Pfam;\s(.*)\n$/){
	    my @split = split(/\;/,$1);
	    print Dumper @split unless scalar(@split)==3;
	    die "Wrong number of elements in pfam-tag." unless scalar(@split)==3;
	    $h->{'pfam'} = $split[0];
	}
	# Get RefSeq
	if($line=~/^DR[\s\t]+(RefSeq.*)\n$/){
	    my @split = split(/\;/,$line);
	    die "Wrong number of elements in RefSeq-tag." unless scalar(@split)==3;
	    $h->{'ref_seq'} = $split[1]." ".$split[2];
	}
	# Get sequence
	if($line=~/^SQ[\s\t]+SEQUENCE.*\n$/){
	    $seq = 1;
	    next;
	}
	if($line=~/^\/\/.*\n$/){
	    $seq = 0;
	}
	if($seq){
	    if($h->{'sequence'}){
		$h->{'sequence'} = ($h->{'sequence'}).$line;
	    }
	    else{
		$h->{'sequence'} = $line;
	    }
	}
    }
    foreach(keys(%$h)){
	if($h->{$_}=~/^\s+(.*)[\.\n]+$/){
	    $h->{$_} = $1;
        }
    }
    my $s = $h->{'sequence'};
    $s=~s/\t//g;
    $s=~s/\s//g;
    $h->{'sequence'} = $s;
    $h->{'id'} = $id;
    $h->{'type'} = "uniprot";
    _printFasta(*FASTA,$id,$h->{'sequence'},$taxon);
    close(FASTA);

    my $dump = Data::Dumper->new([$h],[qw($annotation)])->Purity(1)->Dump();
    my $pointer = tell(ANNO);
    my $length = length($dump);
    print ANNO $dump;
    my $value = "$pointer||$length";
    $values->{$id} = $value;
    _printAnnotation($annotationDirName,$values);
    close(ANNO);
}

sub _usage{
    print STDOUT "\nScript takes a *.dat file, e.g. bacteria.dat.gz, and outputs two files: a fasta file and an annotation file.\n";
    print STDOUT "Parameter:\n";
    print STDOUT "f : path to *.dat file\n";
    print STDOUT "d : directory where the result files will be stored.\n";
    print STDOUT "h : show this usage.\n";
    print STDOUT "\n\n----------- Result file formats --------------\n";
    print STDOUT "\n-- FASTA FILE --\n";
    print STDOUT "The fasta file contains the protein sequences. Names are given as:\n";
    print STDOUT "    >Uniprot_ID  TAXONOMY.\n";
    print STDOUT "where TAXONOMY is used to identify the correlating annotation file.\n\n";
    print STDOUT "-- ANNOTATION FILE --\n";
    print STDOUT "The annotation file is in XML-format and consists of all annotations found in the *.dat.gz file.\n\n";
    exit;
}
