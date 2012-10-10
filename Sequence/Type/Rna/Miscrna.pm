package GePan::Sequence::Type::Rna::Miscrna;
use base qw(GePan::Sequence::Type::Rna);
=head1 NAME

GePan::Sequence::Type::Rna::Miscrna

=head1 DESCRIPTION

Subclass of class GePan::Sequence::Type::Rna. 

Class providing data for miscRNA sequences.

=head1 ATTRIBUTES

type: type of sequence (fixed to miscrna)

=head1 GETTER & SETTER METHODS

=head2 B<getType()>

Returns 'miscrna'.

=cut

sub getType{
    return "miscrna";
}

1;
