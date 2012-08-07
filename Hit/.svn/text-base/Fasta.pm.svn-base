package GePan::Hit::Fasta;
use base qw(GePan::Hit);
use Data::Dumper;
use GePan::Logger;

=head1 NAME

GePan::Hit::Fasta

=head1 DESCRIPTION

Class for a single Fasta-hit

=head1 ATTRIBUTES

hit_length = alignment length

identical = percent of identical based on alignment length

similar = number of similar residues based on fastas '%_sim' value and query_length

percent_similarity = percent similarity based on 'similar' and query_length

percent_identity = percent identity 

=head1 METHODS

=head1 GETTER & SETTER METHODS

=head2 B<setHitLength(hit_length)>

Sets total length of all hsp-alignments of hit

=cut

sub setHitLength{
    my ($self,$l) = @_;
    $self->{'hit_length'} = $l;
}

=head2 B<getHitLength()>

Returns total length of all hsps found for hit

=cut

sub getHitLength{
    my $self = shift;
    return $self->{'hit_length'};
}

=head2 B<setIdentical(identical)>

Sets identical residues of match and query. 

=cut

sub setIdentical{
    my ($self,$i) = @_;
    $self->{'identical'} = $i;
}

=head2 B<getIdentical()>

Returns identical residues of match and query

=cut

sub getIdentical{
    my $self = shift;
    return $self->{'identical'};
}

=head2 B<setPercentIdentity(percent_identity)>

Sets percent dentity of hit

=cut

sub setPercentIdentity{
    my ($self,$p) = @_;
    $self->{'percent_identity'} = $p;
}

=head2 B<getPercentIdentity()>

Returns percent identity of hit

=cut

sub getPercentIdentity{
    my $self = shift;
    return $self->{'percent_identity'};
}


=head2 B<setPercentSimilarity(percent_similarity)>

Sets percent dentity of hit

=cut

sub setPercentSimilarity{
    my ($self,$p) = @_;
    $self->{'percent_similarity'} = $p;
}

=head2 B<getPercentSimilarity()>

Returns percent identity of hit

=cut

sub getPercentSimilarity{
    my $self = shift;
    return $self->{'percent_similarity'};
}


=head2 B<getName()>

Returns 'Signalp';

=cut

sub getToolName{
    return 'Fasta';
}


=head1 INTERNAL METHODS

=head2 B<_significant()>

Implementation of abstract method SUPER::_significant();

Fasta-hit is considered significant if:

1. percent_identity >=30% or percent_similarity>=40%

AND

2. e-value <= 1e-5 

More detailed classification is done when transferring annotation.


=cut


sub _significant{
    my $self = shift;

    if(!$self->{'annotation'}){
        $self->{'logger'}->LogWarning("No annotation found for significance estimation for fasta-hit \'".$self->getID()."\'");
        return 0;
    }
    if(!(exists($self->{'e_value'}))){
        $self->{'logger'}->LogWarning("No e-value found for significance estimation for fasta-fit \'".$self->getID()."\'.");
        return 0;
    }
    if(!(exists($self->{'percent_identity'}))){
        $self->{'logger'}->LogWarning("No percent-identity given for significance estimation for fasta-hit \'".$self->getID()."\'");
        return 0;
    }
    if(!(exists($self->{'percent_similarity'}))){
	$self->{'logger'}->LogWarning("No percent-similarity given for significance estimation for fasta-hit \'".$self->getID()."\'.");
    }

    my $sig = 0;

    if($self->{'e_value'} <= "1e-5"){
        if(($self->{'percent_identity'}>=30)||($self->{'percent_similarity'}>=40)){
            $sig = 1;
        }
        else{
	    $sig = 0;
        }
    }
    else{
	$sig = 0;
    }
    $self->{'significance'} = $sig;
}


=head2 B<_getAttributes()>

Returns a list of all attribute fields of object.

=cut

sub _getAttributes{
    my $self = shift;
    return ['complete_name','percent_identity','percent_similarity','score','length','e_value','hit_length','identical','similar','significance'];
}


1;
