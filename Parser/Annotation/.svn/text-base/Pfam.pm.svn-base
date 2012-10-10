package GePan::Parser::Annotation::Pfam;
use base qw(GePan::Parser::Annotation);

use strict;
use GePan::Collection::Hit;
use GePan::Hit::Pfam;
use Data::Dumper;
use GePan::Logger;

=head1 NAME

GePan::Parser::Annotation::Pfam

=head1 DESCRIPTION

Class for parsing pfam-tool-output.

The file to parse must be obtained by a hmmsearch-call using the '--domtblout' option

=head1 METHODS

=head2 B<parseFile()>

Parses file to create list of hits

=cut

sub parseFile{
    my $self = shift;
    my $file = $self->{'file'};
    $self->{'logger'}->LogError("No Pfam result-file given to parse.") unless $file;

    $self->{'logger'}->LogError("No database given.") unless $self->{'database'};
    my $db = $self->{'database'};

    my $collection = GePan::Collection::Hit->new();

    open(FILE,"<$file") or $self->{'logger'}->LogError("Failed to open Pfam result file $file for reading.");
    while(<FILE>){
	my $line = $_;
	next unless $line!~/^#.*$/;
	my @split = grep{$_ ne '';}split(/\s/,$line);
	$self->{'logger'}->LogError("Wrong number of elements in line-split.") unless scalar(@split)>=23;
	
	my $last = '';
	while(scalar(@split)>=23){
	    my $t = pop(@split);
	    $last="$t $last";
	}
	my $hit = GePan::Hit::Pfam->new();
	my $params = {id=>$split[0],
		      query_length=>$split[5],
		      query_name=>$split[3],
		      accession_number=>$split[1],
		      e_value=>$split[6],
		      score=>$split[7],
		      bias=>$split[8],
		      'length'=>$split[2],
		      domain_evalue=>$split[12],
		      domain_score=>$split[13],
		      domain_bias=>$split[14],
		      logger=>$self->{'logger'},
		      domain_start=>$split[15],
		      domain_stop=>$split[16],
		      query_start=>$split[17],
		      query_stop=>$split[18],
		      accuracy=>$split[21],
		      domain_total=>$split[10],
		      database=>$db,
		      tool=>"pfam",
		      domain_num=>$split[9]};
	$hit->setParams($params);
	$collection->addElement($hit);
    }
    $self->{'collection'} = $collection;  
}


1;
