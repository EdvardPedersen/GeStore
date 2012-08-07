package GePan::Annotation::Blast;
use base qw(GePan::Annotation);

use strict;

=head1 NAME

GePan::Annotation:Blast:

=head1 DESCRIPTION

Class of all annotations of blast-formated databases.

=head1 ATTRIBUTES

pfam : Pfam accession number

pir : Protein Information Resource IDs

taxonomy_id : Taxonomy ID of sequence

embl : Embl accession number

ref_seq : ref_seq ID

=head1 GETTER & SETTER METHODS

=head2 B<setPfam(pfam)>

Sets Pfam-ID of sequence.

=cut

sub setPfam{
    my ($self,$value) = @_;
    $self->{'pfam'} = $value;
}

=head2 B<getPfam()>

Returns pfam-ID of sequence.

=cut

sub getPfam{
    my $self = shift;
    return $self->{'pfam'};
}

=head2 B<setEmbl>

Sets Embl-ID of sequence.

=cut

sub setEmbl{
    my ($self,$value) = @_;
    $self->{'embl'} = $value;
}

=head2 B<getEmbl()>

Returns Embl-ID of sequence.

=cut

sub getEmbl{
    my $self = shift;
    return $self->{'Embl'};
}

=head2 B<setTaxonomyID(taxonomy_id)>

Sets taxonomy_id of sequence.

=cut

sub setTaxonomyID{
    my ($self,$value) = @_;
    $self->{'taxonomy_id'} = $value;
}

=head2 B<getTaxonomyID()>

Returns taxonomy_id of sequence.

=cut

sub getTaxonomyID{
    my $self = shift;
    return $self->{'taxonomy_id'};
}

=head2 B<setRefSeq(ref_seq)>

Sets Ref-Seq-ID of sequence.

=cut

sub setRefSeq{
    my ($self,$value) = @_;
    $self->{'ref_seq'} = $value;
}

=head2 B<getRefSeq()>

Returns ref_seq ID of sequence.

=cut

sub getRefSeq{
    my $self = shift;
    return $self->{'ref_seq'};
}

=head2 B<setPIR(pir)>

Sets Protein Information Resource ID of sequence.

=cut

sub setPIR{
    my ($self,$value) = @_;
    $self->{'pir'} = $value;
}

=head2 B<getPIR()>

Returns Protein Information Resource ID for sequence.

=cut

sub getPIR{
    my $self = shift;
    return $self->{'pir'};
}

=head2 B<setOrganism(organism)>

Sets name of the organism of sequence.

=cut

sub setOrganism{
    my ($self,$value) = @_;
    $self->{'organism'} = $value;
}

=head2 B<getOrganism()>

Returns organism name of sequence.

=cut

sub getOrganism{
    my $self = shift;
    return $self->{'organism'};
}


=head2 B<_getAttributes()>

Returns a list of all attribute fields of object.

=cut

sub _getAttributes{
    my $self = shift;
    return ["organism","taxonomy_id","ref_seq","embl","id","confidence_level","description"];
}



1;
