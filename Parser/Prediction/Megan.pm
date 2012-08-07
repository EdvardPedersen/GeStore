package GePan::Parser::Prediction::Megan;
use base qw(GePan::Parser::Prediction);
use strict;
use Data::Dumper;
use GePan::Hit::Priam;

=head1 NAME

GePan::Parser::Prediction::Megan

=head1 DESCRIPTION

Parser for tool Megan

=head1 ATTRIBUTE

=head1 METHODS 

=head2 B<parseFile()>

Parses the result file from Megan.

=cut

sub parseFile{
    my $self = shift;

    $self->{'logger'}->LogError("No priam output file specified for parsing.") unless $self->{'file'};

    my $collection = GePan::Collection::Hit->new();
    $collection->setLogger($self->{'logger'});

    my $file = $self->{'file'};
    open(FILE,"<$file") or $self->{'logger'}->LogError("Failed to open file $file for reading.");
    my $header;
    while(my $line = <FILE>){
	if($line=~/^>(.*)$/){
	    $header = $1;
	    next;
	}   	
	my @split = grep{$_ ne ""}split(/[\t\s]/,$line);
	$self->{'logger'}->LogError("GePan::Parser::Prediction::Signalp::parseFile() - Odd nmber of elements in line split.") unless scalar(@split)<=5;


	my $hit = GePan::Hit::Priam->new();

	$hit->setParams({ec=>$split[0],
		         e_value=>$split[1],
		         probability=>$split[2],
		         kept=>$split[3],
		         fragment=>'No',
			 id=>$header,
			 logger=>$self->{'logger'}});
	if (defined $split[4]){
	   $hit->setParams({fragment=>$split[4]});
	
	}

	$collection->addElement($hit);
    }
    close(FILE);
    $self->{'collection'} = $collection;
}

1;
