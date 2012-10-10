package GePan::Hit::Blast;
use base qw(GePan::Hit);
use strict;
use GePan::Logger;

=head1 NAME

GePan::Hit::Blast

=head1 DESCRIPTION

Class for a single Blast-hit

=head1 ATTRIBUTES

hit_num = hit number of query sequence

hit_db = name of db_file (e.g. uniprot_sprot_archaea)

hsp_num = number of hsps found 

hit_length = length-sum of all hsps-alignments found

identical = number of identical residues of all hsps

positives = number of residues with positive score in all hsps.

percent_similarity = percent similarity (percent of positive residues in query sequence based on 'positives')

percent_identity = percent identity (percent of identical residues in query sequence based on 'identical')

complete_name = complete name of the hit (id takes just the first part)

    NOTE: For tabular blast output percent_identity is just for alignment_length (no query-sequence length given)

=head1 METHODS

=head1 GETTER & SETTER METHODS

=head2 B<setHitNum(hit_num)>

Sets the number of this hit

=cut	

sub setHitNum{
    my ($self,$num) = @_;
    $self->{'hit_num'} = $num;
}

=head2 B<getHitNum()>

Returns number of this hit

=cut

sub getHitNum{
    my $self = shift;
    return $self->{'hit_num'};
}

=head2 B<setHitDB(hit_db)>

Sets name of the database file of hit (e.g. uniprot_sprot_archaea)

=cut

sub setHitDB{
    my ($self,$db) = @_;
    $self->{'hit_db'} = $db;
}

=head2 B<getHitDB()>

Returns name of database file of hit (e.g.uniprot_sprot_archaea)

=cut

sub getHiDB{
    my $self = shift;
    return $self->{'hit_db'};
}

=head2 <setHSPNum(hsp_num)>

Set number of hsps found for hit

=cut

sub setHSPNum{
    my ($self,$num) = @_;
    $self->{'hsp_num'} = $num;
}

=head2 B<getHSPNum()>

Return number of hsps found for hit

=cut

sub getHSPNum{
    my $self = shift;
    return $self->{'hsp_num'};
}

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
    return 'Blast';
}


=head2 B<getCompleteName()>

Returns the complete name of hit.

=cut

sub getCompleteName{
    my $self = shift;
    return $self->{'complete_name'};
}

=head2 B<setCompleteName()>

Sets complete name of hit.

=cut

sub setCompleteName{
    my ($self,$name) = @_;
    $self->{'complete_name'} = $name;
}



=head1 INTERNAL METHODS

=head2 B<_significant()>

Implementation of abstract method SUPER::_significant();

A hit is considered significant if:

1. Hit identity >=30% or  similarity >=40%

AND

2. e-value <= 1e-5

More detailed classification is done when transferring annotation.

=cut

sub _significant{
    my $self = shift;
    

    if(!$self->{'annotation'}){
        $self->{'logger'}->LogWarning("No annotation found for significance estimation for blast-hit \'".$self->getID()."\'");
        return 0;
    }   
    if($self->getAnnotation()->getDescription()=~m/annotation\sfound/i){
	return 0;
    }
    if(!(exists($self->{'e_value'}))){
        $self->{'logger'}->LogWarning("No e-value found for significance estimation for blast-fit \'".$self->getID()."\'.");
        return 0;
    }
    if(!(exists($self->{'percent_identity'}))){
        $self->{'logger'}->LogWarning("No percent-identity given for significance estimation for blast-hit \'".$self->getID()."\'");
        return 0;
    }
    if(!(exists($self->{'percent_similarity'}))){
        $self->{'logger'}->LogWarning("No percent-similarity given for significance estimation for blast-hit \'".$self->getID()."\'.");
	return 0;
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
    return ['complete_name','hit_num','hit_db','hsp_num','hit_length','identical','positives','percent_identity','percent_similarity','score','length','e_value','significance'];
}


1;
