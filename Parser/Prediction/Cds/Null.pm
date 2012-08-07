package GePan::Parser::Prediction::Cds::Null;
use base qw(GePan::Parser::Prediction::Cds);

use strict;
use GePan::PredictionTool::Null;
use GePan::Logger;
use Data::Dumper;

=head1 NAME

GePan::Parser::Prediction::Cds::Glimmer3

=head1 DESCRIPTION

Parser for parsing result file from glimmer3 prediction

Sub-class of GePan::Parser::Prediction::Cds

=cut

=head1 METHODS

=head2 B<parseFile()>

Parses the file specified by $self->{'file'} and creates GePan::Sequence::Type::Cds objects.

=cut 

sub parseFile{
    my $self = shift;

    $self->{'logger'}->LogError("No file given to parse.") unless $self->{'file'};
    my $file = $self->{'file'};

    $self->{'collection'} = GePan::Collection::Sequence->new();
   
    open(FILE,"<$file") or $self->{'logger'}->LogError("Failed to open file $file for reading.");
    my $contig;
    while(<FILE>){
	my $line = $_;
	if($line=~/^>(.*)[\r\s\t\n]*$/){
	    my $tmp = $1;
	    my @split = grep{$_ ne ""}split(/[\s\t]/,$tmp);
	    my $contigName = $split[0];
	    $self->{'logger'}->LogError("No contig name $contigName found in list of contigs.") unless $self->{'parent_sequences'}->getElementByID($contigName);
	    $contig = $self->{'parent_sequences'}->getElementByID($contigName);
	    next;
	}
	my @split = split(" ",$line);
    
	$self->{'logger'}->LogError("Wrong number of elements in result line: $line.") unless scalar(@split)==5;
	my $predictionTool = GePan::PredictionTool::Glimmer3->new();
	my ($frame,$strand);
	if($split[3]=~/([\+\-])(\d)$/){
	    $strand = $1;
	    $frame = $2;
	}
	else{
	    $self->{'logger'}->LogError("Glimmer frame/strand of wrong format: ".$split[3]);
	}

	# excude genes that are predicted over the borders/ends of a contig, e.g. on circular genomes
	if((($strand eq "-")&&($split[1]<$split[2]))||(($strand eq "+")&&($split[1]>$split[2]))){
	    next;
	}



	$predictionTool->setParams({file=>$file,
				    input_file=>$contig->{'file'},
				    score=>$split[4]});
	my $name = $contig->{'id'}."_".$split[0];
	my $params = {start=>$split[1],
		      stop=>$split[2],
		      frame=>$frame,
		      prediction_tool=>$predictionTool,
		      parent_sequence=>$contig->{'id'},
		      id=>$name};
	if($strand eq "-"){
	    $params->{'complement'} = 1;
	}
	else{
	    $params->{'complement'} = 0;
	}

	my $seqObj = GePan::Sequence::Type::Cds->new();
	$seqObj->setParams($params);
	$self->_createSequences($contig->getID(),$seqObj);	
    }
    close(FILE);
}

1;
