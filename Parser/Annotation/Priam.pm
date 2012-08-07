package GePan::Parser::Annotation::Priam;
use base qw(GePan::Parser::Annotation);

use strict;
use GePan::Collection::Hit;
use GePan::Hit::Priam;
use Data::Dumper;
use GePan::Logger;

=head1 NAME

GePan::Parser::Annotation::Priam

=head1 DESCRIPTION

Class for parsing priam-tool-output.


=head1 METHODS

=head2 B<parseFile()>

Parses file to create list of hits

=cut

sub parseFile{
    my $self = shift;
    my $file = $self->{'file'};
    $self->{'logger'}->LogError("No Priam result-file given to parse.") unless $file;

    $self->{'logger'}->LogError("No database given.") unless $self->{'database'};
    my $db = $self->{'database'};

    my $collection = GePan::Collection::Hit->new();

    open(FILE,"<$file") or $self->{'logger'}->LogError("Failed to open Priam result file $file for reading.");
    while(<FILE>){
	my $line = $_;
	next if $line=~/^\d.*$/;
	my @split = grep{$_ ne '';}split(/\s/,$line);
	$self->{'logger'}->LogError("Wrong number of elements in line-split.") unless scalar(@split)>=4;
	
	my $fragment = 'No';
	while(scalar(@split)>=5){
	    my $fragment = pop(@split);
	}
	my $hit = GePan::Hit::Priam->new();
	my $params = {ec=>$split[0],
		      e_value=>$split[1],
		      probability=>$split[2],
		      kept=>$split[3],
		      database=>$db,
		      fragment=>$fragment,
		      tool=>"priam"};
	$hit->setParams($params);
	$collection->addElement($hit);
    }
    $self->{'collection'} = $collection;  
}


1;
