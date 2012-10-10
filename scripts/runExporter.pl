#!/usr/bin/perl

use strict;
use Getopt::Std;
use GePan::Annotator;
use GePan::Logger;
use GePan::Collection::Sequence;
use GePan::ToolRegister;
use Data::Dumper;
use GePan::Parser::Input::Fasta;
use GePan::Exporter::Embl;
use GePan::Exporter::XML::SimpleAnnotation;
use GePan::Config qw(GEPAN_PATH);
use GePan::Exporter::Metarep;

=head1 NAME

    runExporter.pl

=head1 DESCRIPTION

Script reads in all xml files created by runAnnotator.pl and exports annotation in given format.

=head1 PARAMETER

p: path to node working directory

=cut


eval{
    _main();
};
if($@){
    print $@;
}


sub _main{
    our %opts;
    getopts("p:",\%opts);

    my $paramFile = $opts{'p'}."/parameter.xml";
    my $params = _createParams($paramFile);
    $params->{'node_dir'} = $opts{'p'};

    # create logger
    my $logger = GePan::Logger->new();
    $logger->setStatusLog("gepan.log");
    $logger->setNoPrint(1);
    $params->{'logger'} = $logger;

    # create annotated sequences 
    $params->{'sequences'} = _createSequences($params);

    # export annotation
    _runExporter($params);
}



=head2 B<_runExporter()>

Exports annotations with given exporter type:

1: Sequences are exported in embl format. If contigs are given one Embl file per contig is written out. 

2: Simple XML output of all sequences with annotations and best hit is exported

=cut

sub _runExporter{
    my $params = shift;
    
    my $exporter;
    if($params->{'exporter_type'} eq '2'){
        $exporter = GePan::Exporter::XML::SimpleAnnotation->new();
        my $exporterParams = {"output_directory"=>$params->{'node_dir'}."/output",
                               file=>"simpleOutput.xml",
                               logger=>$params->{'logger'}};
                               
        my $cds = GePan::Collection::Sequence->new();
        while(my $seq = $params->{'sequences'}->getNextElement()){
            if(($seq->getType() eq "cds")&&($seq->getSequenceType() eq "nucleotide")){
                $cds->addElement($seq);
            }
        }

        $params->{'logger'}->LogError("No cds sequences found.") unless $cds->getSize();

        $exporterParams->{'collection'} = $params->{'sequences'};

        $exporter->setParams($exporterParams);
        $exporter->export;
    }
    elsif($params->{'exporter_type'} eq '3'){
        $exporter = GePan::Exporter::Metarep->new();
        my $exporterParams = {"output_directory"=>$params->{'node_dir'}."/output",
                               file=>"MetarepResults.txt",
                               logger=>$params->{'logger'}};
                               
        my $cds = GePan::Collection::Sequence->new();
        while(my $seq = $params->{'sequences'}->getNextElement()){
            if(($seq->getType() eq "cds")&&($seq->getSequenceType() eq "nucleotide")){
                $cds->addElement($seq);
            }
        }

        $params->{'logger'}->LogError("No cds sequences found.") unless $cds->getSize();

        $exporterParams->{'collection'} = $params->{'sequences'};

        $exporter->setParams($exporterParams);
        $exporter->export;
    }
    elsif(($params->{'exporter_type'} eq "1a")||($params->{'exporter_type'} eq "1b")){
        # get input contig or read sequences
        my $contigs = _createParentSequences($params);
        $exporter = GePan::Exporter::Embl->new();
        $exporter->setParams({collection=>$params->{'sequences'},
                             parent_collection=>$contigs,
                             logger=>$params->{'logger'},
                             output_directory=>$params->{'node_dir'}."/output"});
        if($params->{'exporter_type'} eq "1a"){
            $exporter->setStrict(1);
        }
        $exporter->export();
    }
    else{
        $params->{'logger'}->LogError("runAnnotator::_runExporter() - Unknown exporter type ".$params->{'exporter_type'});
    }
}



=head2 B<_createSequences()>

Creates sequences from dumped annotated sequences.

=cut

sub _createSequences{
    my $params = shift;
    
    my $path = $params->{'node_dir'}."/input/";

    opendir(DIR,$path) or die "Failed to opend directory $path";
    my @files = grep{$_=~/^.*\.dump/}readdir(DIR);

    my $seqs = GePan::Collection::Sequence->new();
    foreach my $file(@files){
	my $dump="";
	open(DUMP,"<$path/$file") or die "Failed to open file $path/$file for reading.";
	while(<DUMP>){
	    $dump.=$_;
	}   
	close(DUMP);
	my $collection;
	eval $dump;
	while(my $seq = $collection->getNextElement()){
	    $seqs->addElement($seq);
	}
    }

    $seqs->sortByStart();

    return $seqs;
}




sub _createParentSequences{
    my $params = shift;

    # create parent sequences
    my $parentPath = $params->{'node_dir'}."/input/input.fas"; 

    my $reader = GePan::Parser::Input::Fasta->new();
    $reader->setParams({file=>$parentPath,
			logger=>$params->{'logger'},
			type=>'contig'});
    $reader->parseFile();

    return $reader->getCollection();
}


=head2 B<_createParams(string)>

Creates Parameter hash from given parameter string.

=cut

sub _createParams{
    my $file = shift;
    my $parser = XML::Simple->new();
    my $data = $parser->XMLin($file);
    return $data;
}




