package GePan::Tool::Annotation;
use base qw(GePan::Tool);

use strict;
use Data::Dumper;

=head1 NAME

Gepan::Tool::Annotation

=head1 DESCRIPTION

Super-class of all tool that are run for annotation of sequences, e.g. fasta or hmmscan.

=head1 ATTRIBUTES

database : GePan::DatabaseConfig object. 

=head1 METHODS

=head1 GETTER AND SETTER METHODS

=head2 B<setDB()>

Sets database hash of tool

=cut

sub setDB{
    my ($self,$db) = @_;
    $self->{'database'} = $db;
}

=head2 B<getDB()>

Returns database hash of tool.

=cut

sub getDB{
    my $self = shift;
    return $self->{'database'};
}

1;
