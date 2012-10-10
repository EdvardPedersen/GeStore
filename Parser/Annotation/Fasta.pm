package GePan::Parser::Annotation::Fasta;
use base qw(GePan::Parser::Annotation);

use strict;
use Data::Dumper;
use GePan::Hit::Fasta;
use GePan::Collection::Hit;
use GePan::Logger;

=head1 NAME

GePan::Parser::Annotation::Fasta

=head1 DESCRIPTION

Class for parsing fasta-output. Fasta-run has to be performed using 'fasta35 -O -L -m 9 -H -B' 

=head1 METHODS

=head2 B<parseFile()>

Parses the specified file and creates the list of GePan::Hit::Fasta-objects;

=cut

sub parseFile{
    my $self = shift;
    
    $self->{'logger'}->LogError("No fasta output file specified for parsing.") unless $self->{'file'};

    my $seqs = GePan::Collection::Hit->new();

    my $file = $self->{'file'};
    my ($new,$queryName,$queryLength);
    open(FILE,"<$file") or $self->{'logger'}->LogError("Failed to open file $file for reading.");
    while(<FILE>){
	my $line = $_;
	if($line=~/^\+\-[\t\s]+.*/){
	    next;
	}
	if($line=~/^.*>>>.*$/){
	    my @sp1 = split(/>>>/,$line);
	    $self->{'logger'}->LogError("Odd number of elements in line split >>>") unless scalar(@sp1)==2;
	    if($sp1[1]=~/^(.*)\s*[,-][\s\t]*(\d+)\s.*$/){
		my $tmp = $1;
		$queryName = (grep{$_ ne ""}(split(" ",$tmp)))[0];
		$queryLength = $2;
	    }
	    else{
		$self->{'logger'}->LogError("Fasta-parser query_name regexp doesn't match.");
	    }	
	}
	elsif($new && ($line=~/^[\n\s\t]*$/)){
	    $new = 0;
	    $queryName = '';
	    $queryLength = '';
	}
	elsif($line=~/^The best scores are.*$/){
	    $new = 1;
	}
	elsif($new){
	    $line=~s/[\(\)\n]/ /g;
	    my @split = grep{$_ ne '';}split(" ",$line);
	    # splice out the frame in case fastx was performed
	    if($line=~/^.*[\s\t]+\[[rf]\][\s\t]+.*$/){
		my $fr = splice(@split,3,1);
	    }
	    my $i = $split[6];
	    if(!$queryLength){
		$self->{'logger'}->LogError(Dumper @split);
	    }
	    my $id = int(($split[9])*($split[6]));
	    my $percIdent = sprintf("%.2f",(($id*100)/$queryLength));
	    my $similar = int(($split[9])*($split[7]));
	    my $percSim = sprintf("%.2f",($similar*100)/$queryLength);

	    $self->{'logger'}->LogError("Wrong number of elements in line-split of line \'$line\'") unless scalar(@split) == 21;
	    my $params = {id=>$split[0],
			  database=>$self->{'database'},
			  'length'=>$split[2],
			  e_value=>$split[5],
			  hit_length=>$split[9],
			  identical=>$id,
			  logger=>$self->{'logger'},
			  similar=>$similar,
			  percent_similarity=>$percSim,
			  percent_identity=>$percIdent,
			  tool=>"fasta",
			  query_name=>$queryName,
			  query_length=>$queryLength,
			  z_score=>$split[4]};
	    my $hitObj = GePan::Hit::Fasta->new();
	    $hitObj->setParams($params);
	    $seqs->addElement($hitObj);
	}
	
    }

    close(FILE);
    $self->{'collection'} = $seqs;
}

1;
