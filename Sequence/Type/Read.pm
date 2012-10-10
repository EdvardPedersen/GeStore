package GePan::Sequence::Type::Read;
use base qw(GePan::Sequence::Base::Nucleotide);

=head1 NAME

GePan::Sequence::Type::Read

=head1 DESCRIPTION

Subclass of class GePan::Sequence::Base::Nucleotide to represent reads.

=head1 ATTRIBUTES

type: type of sequence (fixed to read)

collection: 

=head1 GETTER & SETTER METHODS

=head2 B<getType()>

Returns 'read'.

=cut

sub getType{
    return "read";
}

=head2 B<setCollection(GePan::Collection::Sequence)>

Sets GePan::Collection::Sequence object of sequences predicted on this contig.

=cut

sub setCollection{
    my ($self,$c) = @_;
    $self->{'collection'} = $c;
}


=head2 B<getCollection()>

Returns GePan::Collection::Sequence object of sequences predicted on this contig.

=cut

sub getCollection{
    my $self = shift;
    return $self->{'collection'};
}

1;
