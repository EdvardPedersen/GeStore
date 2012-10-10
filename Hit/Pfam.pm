package GePan::Hit::Pfam;
use base qw(GePan::Hit);
use Data::Dumper;
use strict;
use GePan::Logger;

=head1 NAME

GePan::Hit::Pfam

=head1 DESCRIPTION

Class for a single Pfam-hit

=head1 ATTRIBUTES

accession_number = accession number of  domain/family 

bias = bias of domain hit 

query_length = length of query sequence

domain_evalue = e-value of the best domain-hit (independent e-value)

domain_score = score of the particular domain 

domain_bias = bias for the best hit domain 

domain_start = start of match in hmm

domain_stop = stop of match in hmm

query_start = start of match in query

query_stop = stop of match in query

domain_total = number of domains found

domain_num = number of domain in domain_total

accuracy = accuracy of all residues  

=head1 METHODS

=head1 GETTER & SETTER METHODS

=head2 B<setAccessionNumber(name)>

Set gene name of hit 

=cut

sub setAccessionNumber{
    my ($self,$name) = @_;
    $self->{'accession_number'} = $name;
}

=head2 B<getAccessionNumber()>

Returns gene name

=cut

sub getAccessionNumber{
    my $self = shift;
    return $self->{'accession_number'};
}

=head2 B<setBias(bias)>

Sets bias of complete domain hit

=cut

sub setBias{
    my ($self,$b) = @_;
    $self->{'bias'} = $b;
}

=head2 B<getBias()>

Returns bias of the complete domain-hit

=cut

sub getBias{
    my $self = shift;
    return $self->{'bias'};
}

=head2 <setDomainScore(domain_score)>

Sets the score for the best match of this domains hmm

=cut

sub setDomainScore{
    my ($self,$ds) = @_;
    $self->{'domain_score'} = $ds;
}

=head2 B<getDomainScore()>

Return the score for the best match of this domains hmm

=cut

sub getDomainScore{
    my $self = shift;
    return $self->{'domain_score'};
}

=head2 B<setDomainBias(domain_bias)>

Sets the bias for the best match of this domain

=cut

sub setDomainBias{
    my ($self,$db) = @_;
    $self->{'domain_bias'} = $db;
}

=head2 B<getDomainBias()>

Returns the bias for the best match of this domain

=cut

sub getDomainBias{
    my $self = shift;
    return $self->{'domain_bias'};
}

=head2 B<setDomainStart(domain_start)>

Sets the alignment start of domain-hit

=cut

sub setDomainStart{
    my ($self,$ds) = @_;
    $self->{'domain_start'} = $ds;
}

=head2 B<getDomainStart()>

Returns the start of the alignment in domain-hit

=cut

sub getDomainStart{
    my $self = shift;
    return $self->{'domain_start'};
}

=head2 B<setDomainStop(domain_stop)>

Sets the alignment stop of domain-hit

=cut

sub setDomainStop{
    my ($self,$ds) = @_;
    $self->{'domain_stop'} = $ds;
}

=head2 B<getDomainStop()>

Returns the stop of the alignment in domain-hit

=cut

sub getDomainStop{
    my $self = shift;
    return $self->{'domain_stop'};
}

=head2 B<setQueryStart(query_start)>

Sets the alignment start of query-sequence

=cut

sub setQueryStart{
    my ($self,$qs) = @_;
    $self->{'query_start'} = $qs;
}

=head2 B<getQueryStart()>

Returns the start of the alignment in query-sequence

=cut

sub getQueryStart{
    my $self = shift;
    return $self->{'query_start'};
}

=head2 B<setQueryStop(query_stop)>

Sets the alignment stop of query-sequence

=cut

sub setQueryStop{
    my ($self,$qs) = @_;
    $self->{'query_stop'} = $qs;
}

=head2 B<getQueryStop()>

Returns the stop of the alignment in query-sequence

=cut

sub getQueryStop{
    my $self = shift;
    return $self->{'query_stop'};
}

=head2 B<setDomainTotal(domain_total>

Sets the number of domains found for this family

=cut

sub setDomainTotal{
    my ($self,$dt) = @_;
    $self->{'domain_total'} = $dt;
}

=head2 B<getDomainTotal()>

Returns the number of domains found for this family

=cut

sub getDomainTotal{
    my $self = shift;
    return $self->{'domain_total'};
}

=head2 B<setDomainNum(domain_num>

Sets the number of the domain for this family-hit

=cut

sub setDomainNum{
    my ($self,$dn) = @_;
    $self->{'domain_num'} = $dn;
}

=head2 B<getDomainNum()>

Returns the number of the domain found for this family-hit

=cut

sub getDomainNum{
    my $self = shift;
    return $self->{'domain_num'};
}

=head2 B<setAccuracy(accuracy)>

Sets the accuracy for of all residues of the hit

=cut

sub setAccuracy{
    my ($self,$ac) = @_;
    $self->{'accuracy'} = $ac;
}

=head2 B<getAccuracy()>

Returns the accuracy of all residues of the hit

=cut

sub getAccuracy{
    my $self = shift;
    return $self->{'accuracy'};
}

=head2 B<setQueryName(query_name)>

Sets name of query sequence

=cut

sub setQuerySequence{
    my ($self,$qn) = @_;
    $self->{'query_name'} = $qn;
}

=head2 B<getQueryName()>

Returns name of the query sequence

=cut

sub getQueryName{
    my $self = shift;
    return $self->{'query_name'};
}

=head2 B<setDomainEValue(domain_evalue)>

Sets the e-value of the best domain hit of this family (independend e-value)

=cut

sub setDomainEValue{
    my ($self,$de) = @_;
    $self->{'domain_evalue'} = $de;
}

=head2 B<getDomainEValue()>

Returns the independent e-value of the hit

=cut

sub getDomainEValue{
    my $self = shift;
    return $self->{'domain_evalue'};
}

=head2 B<setQueryLength(query_length)>

Sets length of the query sequence

=cut

sub setQueryLength{
    my ($self,$l) = @_;
    $self->{'query_length'} = $l;
}

=head2 B<getQueryLength()>

Returns the length of the query sequence

=cut

sub getQueryLength{
    my $self= shift;
    return $self->{'query_length'};
}

=head1 INTERNAL METHODS

=head2 B<_significant()>

Implementation of abstract method SUPER::_significant();

A hit to a Pfam-database is considered significant if:

1. a gathered threshold for the family/domain is known and the domain hit-score is >= the domains 'gathered threshold' of the given GePan::Annotation::Pfam 

2. no gathered but a trusted cutoff is known and trusted cutoff <= domain hit-score

3. neither GA nor TC are known but the evalue of the domain hit <= 0.0001

=cut

sub _significant{
    my $self = shift;
    if(!$self->{'annotation'}){
	$self->{'logger'}->LogWarning("No annotation given for pfam-hit \'".$self->getID()."\'\n");
	return 0;
    }
    if(!(exists($self->{'domain_score'}))){
	$self->{'logger'}->LogWarning("No domain_score found for pfam-hit \'".$self->getID()."\'\n");
	return 0;
    }
    if(!(exists($self->{'domain_evalue'}))){
	$self->{'logger'}->LogWarning("No domain_score found for pfam-hit \'".$self->getID()."\'\n");
	return 0;
    }

    my $sig = 0;

    # check domain score, e.g. second number in gathered threshold
    if($self->{'annotation'}->{'gathered_threshold'}){
	my @split = grep {$_ ne "";}split(/ /,$self->{'annotation'}->{'gathered_threshold'});
	$self->{'logger'}->LogError("Odd number of elements in ga-split.") unless scalar(@split)==2;
	if($self->{'domain_score'} >= $split[1]){
	    $sig = 1;
	}
    }
    elsif($self->{'annotation'}->{'trusted_cutoff'}){
	my @split = grep{$_ ne "";}split(/ /,$self->{'annotation'}->{'trusted_cutoff'});
	$self->{'logger'}->LogError("Odd number of elements in tc-split.") unless scalar(@split)==2;
	if($self->{'domain_score'} >= $split[1]){
	    $sig = 1;
	}
    }
    else{
	if($self->{'domain_evalue'}<= 0.0001){
	    $sig = 1;
	}
    }
    $self->{'significance'} = $sig;
}





=head2 B<_getAttributes()>

Returns a list of all attribute fields of object.

=cut

sub _getAttributes{
    my $self = shift;
    return ['percent_identity','percent_similarity','score','length','e_value','bias','domain_evalue','domain_score','domain_bias','domain_start','domain_stop','query_start','query_stop','domain_total','significance'];
}


=head2 B<getName()>

Returns 'Signalp';

=cut

sub getToolName{
    return 'Pfam';
}


1;
