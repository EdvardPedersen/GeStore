package GePan::Parser::Prediction::Signalp;
use base qw(GePan::Parser::Prediction);
use strict;
use Data::Dumper;
use GePan::Hit::Signalp;

=head1 NAME

GePan::Parser::Prediction::Signalp

=head1 DESCRIPTION

Parser for tool signalP

=head1 ATTRIBUTE

=head1 METHODS 

=head2 B<parseFile()>

Parses the result file of a signalP run.

=cut

sub parseFile{
    my $self = shift;

    $self->{'logger'}->LogError("No signalp output file specified for parsing.") unless $self->{'file'};

    my $collection = GePan::Collection::Hit->new();
    $collection->setLogger($self->{'logger'});

    my $file = $self->{'file'};
    open(FILE,"<$file") or $self->{'logger'}->LogError("Failed to open file $file for reading.");
    while(my $line = <FILE>){
	if($line=~/^#.*$/){
	    next;
	}   	
	my @split = grep{$_ ne ""}split(/[\t\s]/,$line);
	$self->{'logger'}->LogError("GePan::Parser::Prediction::Signalp::parseFile() - Odd nmber of elements in line split.") unless scalar(@split)==21;

	next unless (($split[3] eq 'Y')||($split[6] eq 'Y')||($split[9] eq 'Y')||($split[11] eq 'Y')||($split[13] eq 'Y')||($split[20] eq 'Y')||($split[18] eq 'Y'));

	my $hit = GePan::Hit::Signalp->new();
	$hit->setParams({cleavage_position=>$split[17],
			 cleavage_probability=>$split[16],
			 signal=>$split[20],
			 signal_probability=>$split[19],
			 cleavage=>$split[18],
			 parent=>$split[14],
			 id=>$split[14],
			 logger=>$self->{'logger'}});
	$collection->addElement($hit);
    }
    close(FILE);
    $self->{'collection'} = $collection;
}

1;
