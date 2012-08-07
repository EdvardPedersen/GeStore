package GePan::Exporter::XML;
use base qw(GePan::Exporter);

use GePan::Collection::Sequence;

use strict;
use Data::Dumper;

=head1 NAME

GePan::Exporter::XML

=head1 DESCRIPTION

Main class for all XML-exporter. Inherits from GePan::Exporter

=head1 ATTRIBUTES

sequences: GePan::Collection::Sequence of annotated sequences

=head1 GETTER & SETTER METHODS

=head2 B<setSequences(GePan::Collection::Sequence)>

Sets self->{'sequence'}

=cut

sub setSequences{
    my ($self,$seqs) = @_;
    $self->{'sequences'} = $seqs;
}

=head2 B<getSequences()>

Returns GePan::Collection::Sequence object.

=cut

sub getSequences{
    my $self = shift;
    return $self->{'sequences'};
}

=head2 B<setParams(ref)>

Sets self->{$key} to ref->{$key} for all keys of ref.

=cut

sub setParams{
    my ($self,$ref) = @_;
    foreach(keys(%$ref)){
	$self->{$_} = $ref->{$_};
    }
}

1;
