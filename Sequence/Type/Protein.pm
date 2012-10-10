package GePan::Sequence::Type::Protein;
use base qw(GePan::Sequence);
use base qw(GePan::Sequence::Base::Predicted);
use base qw(GePan::Sequence::Base::Annotated);

=head1 NAME

GePan::Sequence::Type::Protein

=head1 DESCRIPTION

Subclass of class GePan::Sequence, GePan::Sequence::Base::Predicted and GePan::Sequence::Base::Annotated. 

Represents all amino acid CDS.

=head1 ATTRIBUTES

codon_table = codon table the nucleotide sequence was translated with.

=head1 METHODS

=cut 

use Bio::Seq;

=head1 GETTER & SETER METHODS

=head2 B<setCodonTable(codon_table)>

Sets codon table to codon_table.

=cut

sub setCodonTable{
    my ($self,$c) = @_;
    $self->{'codon_table'} = $c;
}

=head2 B<getCodonTable()>

Returns codon table of sequence.

=cut

sub getCodonTable{
    my $self = shift;
    return $self->{'codon_table'};
}

=head2 B<getType()>

Returns 'cds'.

=cut

sub getType{
    return 'cds';
}

=head2 B<getSequenceType()>

Returns 'protein'.

=cut

sub getSequenceType{
    return 'protein';
}

1;
