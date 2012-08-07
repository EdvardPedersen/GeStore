package GePan::Parser::Prediction::Cds::Mga;
use base qw(GePan::Parser::Prediction::Cds);

use strict;
use GePan::PredictionTool::Mga;
use GePan::Logger;

=head1 NAME

GePan::Parser::Prediction:Cds::Mga

=head1 DESCRIPTION

Parser for parsing result file from MetaGeneAnnotator prediction

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
    my ($contig,$gc);
    while(<FILE>){
	my $line = $_;
	if($line=~/^#.*$/){
	    if(($line=~/^#[\t\s]*gc\s=.*$/)||($line=~/^#[\t\s]*self.*$/)){
	    }
	    elsif($line=~/^#\s(.*)[\s\t\n]*$/){
		# Get just the part of the name before the first whitespace ...
		my $tmp = $1;
		my @split = grep{$_ ne ""}split(/[\s\t]/,$tmp);
		my $contigName = $split[0];
		if($contigName=~/^[\s\t]*([_\-a-zA-Z0-9]+)[\s\t]+[a-zA-Z0-9]+.*$/){
		    $contigName=$1;
		}
		$self->{'logger'}->LogError("No contig name found with name $contigName.") unless $self->{'parent_sequences'}->getElementByID($contigName);
		$contig = $self->{'parent_sequences'}->getElementByID($contigName);
	    }
	    next;
	} 

	my @split = split(/\t/,$line);
    
	$self->{'logger'}->LogError("Wrong number of elements in result line: $line.") unless scalar(@split)==11;
	my $predictionTool = GePan::PredictionTool::Mga->new();
	my $frame = ($split[4]);
	my $strand = $split[3];
	my $complete = $split[5];
	my $start = $strand eq '-'?$split[2]:$split[1];
	my $stop = $strand eq '-'?$split[1]:$split[2];

	if(($complete eq '01')||($complete eq '00')){
	    if($frame){
		if($strand eq '+'){
		    $start+=$frame;
		}
		else{
		    $start-=$frame;
		}
	    }
	}

	$predictionTool->setParams({file=>$file,
				    input_file=>$contig->{'file'},
				    score=>$split[6],
				    complete=>$split[5],
				    model=>$split[7],
				    rbs_start=>$split[8],
				    rbs_stop=>$split[9],
				    rbs_score=>$split[10]});
	my $name = $contig->{'id'}."_".$split[0];
	my $params = {strand=>$strand,
		      frame=>$frame,
		      start=>$start,
		      stop=>$stop,
		      parent_sequence=>$contig->{'id'},
		      prediction_tool=>$predictionTool,
		      id=>$name,
		      logger=>$self->{'logger'}};
	if($strand eq "-"){
	    $params->{'complement'} = 1;
	}

	my $seqObj = GePan::Sequence::Type::Cds->new();
	$seqObj->setParams($params);
	$self->_createSequences($contig->getID(),$seqObj);	
    }
    close(FILE);
}

1;
