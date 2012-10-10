package GePan::Hit::Signalp;
use base qw(GePan::Hit);
use strict;
use GePan::Logger;

=head1 NAME

GePan::Hit::SignalP

=head1 DESCRIPTION

Class for a single SignalP-hit

=head1 ATTRIBUTES

signal_probability: probability of beeing a signal peptide

signal: Y if signal peptide

cleavage_probability: probability of having a cleavage site

cleavage_position: position of the aa after cleavage site

cleavage: Y diff a cleavage site was found;

=head1 METHODS

=head1 GETTER & SETTER METHODS

=head2 B<setSignal(int)>

Sets 1 if peptide is signal peptide, 0 otherwise

=cut

sub setSignal{
    my ($self,$v);
    $self->{'signal'} = $v;
}

=head2 B<getSignal>

Returns 1 if hit is signal peptide, 0 otherwise.

=cut

sub getSignal{
    my $self = shift;
    return $self->{'Signal'};
}

=head2 B<setCleavage(int)>

Sets 1 if cleavage site was found, 0 otherwise.

=cut

sub setCleavage{
    my ($self,$v);
    $self->{'cleavage'} = $v;
}

=head2 B<getCleavage>

Returns 1 if cleavae site was found, 0 otherwise.

=cut

sub getCleavage{
    my $self = shift;
    return $self->{'cleavage'};
}

=head2 B<setSignalProbability(int)>

Sets probability of peptide beeing a signal peptide.

=cut

sub setSignalProbability{
    my ($self,$v);
    $self->{'signal_probability'} = $v;
}

=head2 B<getSignalProbability()>

Returns probability of peptide beeing a signal peptide

=cut

sub getSignalProbability{
    my $self = shift;
    return $self->{'signal_probability'};
}

=head2 B<setCleavageProbability(int)>

Sets probability of a found cleavage site.

=cut

sub setCleavageProbability{
    my ($self,$v);
    $self->{'cleavage_probability'} = $v;
}

=head2 B<getCleavageProbability()>

Returns probability of a found cleavage side.

=cut

sub getCleavageProbability{
    my $self = shift;
    return $self->{'cleavage_probability'};
}

=head2 B<setCleavagePosition(pos)>

Sets position of found cleavage site.

=cut

sub set{
    my ($self,$v);
    $self->{'cleavage_position'} = $v;
}

=head2 B<getCleavagePosition()>

Returns

=cut

sub getCleavagePosition{
    my $self = shift;
    return $self->{'cleavage_position'};
}

=head2 B<_getAttributes()>

Returns a list of all attribute fields of object.

=cut

sub _getAttributes{
    my $self = shift;
    return ['signal_probability','cleavage_probability','signal','cleavage','cleavage_position'];
}


=head2 B<getName()>

Returns 'Signalp';

=cut

sub getToolName{
    return 'SignalP';
}

1;
