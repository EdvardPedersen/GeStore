package GePan::Parser::Prediction;
use base qw(GePan::Parser);

use strict;
use Data::Dumper;
use GePan::Collection::Sequence;

=head1 NAME

GePan::Parser::Prediction

=head1 DESCRIPTION

Main-class of parsers for prediction tools

Sub-class of GePan::Parser;

=head1 ATTRIBUTE

parent_sequences: GePan::Collection::Sequence of sequences the prediction was run on (e.g. contigs for gene-prediction tools)

collection: GePan::Collection::Sequence object of predicted sequences.

=cut

=head1 GETTER & SETTER METHODS

=head2 B<setParentSequences(GePan::Collection::Sequence)>

Sets GePan::Collection::Sequence object of sequences the prediction is based on.

=cut

sub setParentSequences{
    my ($self,$p) = @_;
    $self->{'parent_sequences'} = $p;
}

=head2 B<getParentSequences()>

Returns GePan::Collection::Sequence object of sequnences the prediction is based on.

=cut

sub getParentSequences{
    my $self = shift;
    return $self->{'parent_sequences'};
}

=head2 B<getCollection()>

Returns Collection::Sequence object of predicted sequences

=cut

sub getCollection{
    my $self = shift;
    return $self->{'collection'};
}

=head1 INTERNAL METHODS

=head2 B<_createSequences()>

Abstract class has to be implemented in sub-classes.

Creates nucleotide and, if set, amino acid sequences and gives a name to the sequence: CONTIG_NAME."_".orfXX

=cut

sub _createSequences{
    my $self = shift;
    $self->{'logger'}->LogError("Abstract class \'_createSequences()\' not implemented.");
}

1;
