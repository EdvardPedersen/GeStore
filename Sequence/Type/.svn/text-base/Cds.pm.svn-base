package GePan::Sequence::Type::Cds;
use base qw(GePan::Sequence::Base::Nucleotide);
use base qw(GePan::Sequence::Base::Predicted);
use base qw(GePan::Sequence::Base::Annotated);

=head1 NAME

GePan::Sequence::Type::Cds

=head1 DESCRIPTION

Subclass of class GePan::Sequence::Nucleotide, GePan::Sequence::Base::Annotated and GePan::Sequence::Base::Predicted. 

Class providing data of cds sequences.

=head1 ATTRIBUTES

frame: frame the Cds was predicted on

complement: 1 if Cds is complement, 0 otherwise.

type: type of sequence (fixed to cds)

=head1 GETTER & SETTER METHODS

=head2 B<setFrame(int)>

Sets frame the CDS was predicted on.

=cut

sub setFrame{
    my ($self,$f) = @_;
    $self->{'frame'} = $f;
}

=head2 B<getFrame()>

Returns frame of CDS.

=cut

sub getFrame{
    my $self = shift;
    return $self->{'frame'};
}


=head2 B<setComplement(int)>

Sets value for complement of CDS: 1 if complement, 0 otherwise.

=cut

sub setComplement{
    my ($self,$c) = @_;
    $self->{'complement'} = $c;
}

=head2 B<getComplement()>

Returns complement of CDS: 1 if complement, 0 otherwise.

=cut

sub getComplement{
    my $self = shift;
    return $self->{'complement'};
}


=head2 B<getType()>

Returns 'cds'.

=cut

sub getType{
    return "cds";
}

1;
