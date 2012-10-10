package GePan::Parser::Annotation::Blast;
use base qw(GePan::Parser::Annotation);

use strict;
use XML::Simple;
use GePan::Hit::Blast;
use GePan::Collection::Hit;
use Data::Dumper;
use GePan::Logger;

=head1 NAME

GePan::Parser::Annotation::Blast

=head1 DESCRIPTION

Class for parsing blast-output in XML format (-m 7) 

=head1 ATTRIBUTES

format : format of balst output. Just '7' supported, yet.

=head1 METHODS

=head2 B<parseFile()>

Parses the file specified by self->{'file'} and format self->{'format'}

=cut

sub parseFile{
    my $self = shift;

    $self->{'logger'}->LogError("No blast-output file given.") unless $self->{'file'};

    my $file = $self->{'file'};

    if(!($self->{'format'})){
#	$self->{'logger'}->LogWarning("Blast parser format automatically set to \'7\'");
	$self->{'format'} = 7;
    }

    if($self->{'format'} == '7'){
	_readXML($self);
    }
    else{
	my $f = $self->{'format'};
	$self->{'logger'}->LogError("Unknown given format \'".$f."\' for blast-output file.");
    }
}



=head1 INTERNAL METHODS

=head2 B<_readXML(self)>

Parses blast output file of XML format

=cut

sub _readXML{
    my $self = shift;

    my $parser = XML::Simple->new();
    my $data = $parser->XMLin($self->{'file'});



    $self->{'blast_program'} = $data->{'BlastOutput_program'};

    my $seqs = GePan::Collection::Hit->new();
    
    my $its = $data->{'BlastOutput_iterations'}->{'Iteration'};

    if($its=~m/HASH/){
	$its = [$its];
    }

    foreach(@$its){
	my $it = $_;
	my $queryName = $it->{'Iteration_query-def'};
	my $queryLength = $it->{'Iteration_query-len'};

	my $hits = $it->{'Iteration_hits'}->{'Hit'};
	if(($hits)&&($hits=~m/HASH/)){
	    $hits = [$hits];
	}

	foreach(@$hits){
	    my $hitObj = GePan::Hit::Blast->new();
	
	    my $h = $_;

	    my $id_tmp = $h->{'Hit_def'};
	    my @id_split = split(/\s/,$id_tmp);
	    $self->{'logger'}->LogError("Wrong number of elements in split of blast Hit_def") unless scalar(@id_split)>=2;

	    my @db_split = split(" ",$h->{'Hit_def'});

	    my %params = (id=>$id_split[0],
			  complete_name=>$id_tmp,
			  hit_num=>$h->{'Hit_num'},
			  database=>$self->{'database'},
			  'length'=>$h->{'Hit_len'},
			  logger=>$self->{'logger'},
			  'hit_db'=>$db_split[-1]);


	    # get all hsps of hit
	    my $hsps = $h->{'Hit_hsps'}->{'Hsp'};
	    if($hsps=~m/HASH/){
		$hsps = [$hsps];
	    }

	    # get score, length, identity and evalue of all hsps
	    my $hit_len = 0;
	    my $e_value = 0;
	    my $identical = 0;
	    my $positives = 0;
	    my $score = 0;

	    foreach(@$hsps){
		$hit_len += $_->{'Hsp_align-len'};
		$e_value +=$_->{'Hsp_evalue'};
		$identical +=$_->{'Hsp_identity'};
		$positives +=$_->{'Hsp_positive'};
		$score+= $_->{'Hsp_score'};
	    }

	    $e_value = $e_value/(scalar(@$hsps));
	    $score = $score/(scalar(@$hsps));
	    my $percIdent = sprintf("%.2f",(($identical*100)/$queryLength));

	    my $percSim = sprintf("%.2f",(($positives*100)/$queryLength));

	    $params{'query_name'} = $queryName;
	    $params{'hsp_num'} = scalar(@$hsps);
	    $params{'percent_identity'} = $percIdent;
	    $params{'percent_similarity'} = $percSim;
	    $params{'identical'} = $identical;
	    $params{'hit_length'} = $hit_len;
	    $params{'e_value'} = $e_value;
	    $params{'score'} = $score;
	    $params{'tool'} = "blast";
	    $params{'positives'} = $positives;
	    $hitObj->setParams(\%params);
	    $hitObj->setLogger($self->{'logger'});
	    $seqs->addElement($hitObj);
	}
    }
    $self->{'collection'} = $seqs;
}


=head1 GETTER & SETTER METHODS

=head2 B<setFormat(format)>

Sets format of blast output.

=cut 

sub setFormat{
    my ($self,$f) = @_;
    $self->{'format'} = $f;
}

=head2 B<getFormat()>

Returns format of blast output

=cut

sub getFormat{
    my $self=  shift;
    return $self->{'format'};
}    


1;
