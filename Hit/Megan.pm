package GePan::Hit::Megan;
use base qw(GePan::Hit);
use Data::Dumper;
use strict;
use GePan::Logger;

=head1 NAME

GePan::Hit::Pfam

=head1 DESCRIPTION

Class for a single Megan-hit

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

=head2 B<setEC(name)>

Set gene name of hit 

=cut

sub setEC{
    my ($self,$name) = @_;
    $self->{'ec'} = $name;
}

=head2 B<getEC()>

Returns Enzyme Commission number

=cut

sub getEC{
    my $self = shift;
    return $self->{'ec'};
}

=head2 B<setProbability(probability)>

Sets probability of EC-hit

=cut

sub setProbability{
    my ($self,$b) = @_;
    $self->{'probability'} = $b;
}

=head2 B<getBias()>

Returns the probability of EC-hit

=cut

sub getProbability{
    my $self = shift;
    return $self->{'probability'};
}

=head2 <setKept(domain_score)>

Sets information if the hit is kept or not (T/F)

=cut

sub setKept{
    my ($self,$ds) = @_;
    $self->{'kept'} = $ds;
}

=head2 B<getKept()>

Returns information if the hit is kept or not (T/F)

=cut

sub getKept{
    my $self = shift;
    return $self->{'kept'};
}

=head2 B<setDomainBias(domain_bias)>

Sets information about whether the hit is part of a fragment or not (No/fragment)

=cut

sub setFragment{
    my ($self,$db) = @_;
    $self->{'fragment'} = $db;
}

=head2 B<getFragment()>

Returns information about whether the hit is part of a fragment or not (No/fragment)

=cut

sub getFragment{
    my $self = shift;
    return $self->{'fragment'};
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
    return ['ec','e_value','probability','kept','fragment','significance'];
}


=head2 B<getToolName()>

Returns 'Priam';

=cut

sub getToolName{
    return 'Priam';
}


1;
