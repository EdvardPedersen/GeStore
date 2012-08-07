package GePan::Sequence::Base::Nucleotide;
use base qw(GePan::Sequence);

=head1 NAME

GePan::Sequence::Nucleotide

=head1 DESCRIPTION

Subclass of class GePan::Sequence. 

One of the basic sequence objects. All sequence types that also have a nucleotide sequence have to inherite from this class.

=head1 ATTRIBUTES

=head1 METHODS

=cut 

use Bio::Seq;

=head2 B<translateSequence(codon_table)>

Returns the protein sequence of this object. Codon table is either given one, or default (11).

=cut


sub translateSequence{
    my ($self,$ct) = @_;

    my $codon = $ct?$ct:11;

    my $seqObj;
    # Note: leading or ending nucleotides are already removed!
    $seqObj = Bio::Seq->new(-id=>$self->{'name'},
                            -seq=>$self->{'sequence'},
                            -alphabet=>'dna',
                            -codontable_id=>$ct);
    return ($seqObj->translate->seq);
}


=head1 GETTER & SETTER METHODS

=head2 B<getSequenceType()>

Returns 'nucleotide';

=cut

sub getSequenceType{
    return 'nucleotide';
}

1;
