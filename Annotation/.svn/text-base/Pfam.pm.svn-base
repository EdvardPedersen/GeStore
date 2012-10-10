package GePan::Annotation::Pfam;
use base qw(GePan::Annotation);

use strict;

=head1 NAME

GePan::Annotation:Pfam:

=head1 DESCRIPTION

Class of all Pfam annotations.

=head1 ATTRIBUTES

accession: Pfam accession number

type: Type of hit, e.g. domain or family

gathering_threshold: gathering threshold (GA) of Pfam domain/family/clade

db_comment: description of the hit-domain

=head1 GETTER & SETTER METHODS

=head2 B<setType(type)>

Set type of annotation.

=cut

sub setType{
    my ($self,$type) = @_;
    $self->{'type'} = $type;
}

=head2 B<getType()>

Returns type of annotation

=cut

sub getType{
    my $self = shift;
    return $self->{'type'};
}

=head2 B<setAccession(accession)>

Sets accession number of annotation to accession.

=cut

sub setAccession{
    my ($self,$acc) = @_;
    $self->{'accession'} = $acc;
}

=head2 B<getAccession()>

Return accession number of annotation object.

=cut

sub getAccession{
    my $self=  shift;
    return $self->{'accession'};
}

=head2 B<setGA(gathering_threshold)>

Sets the gathering threshold of this annotation object (Pfam 'class')

=cut

sub setGA{
    my  ($self,$ga) = @_;
    $self->{'gathering_threshold'} = $ga;
}

=head2 B<getGA()>

Returns the gathering threshold of this annotation object (Pfam 'class')

=cut

sub getGA{
    my $self = shift;
    return $self->{'gathering_threshold'};
}

=head2 B<getDBComment()>

Returns the database comment of the hit.

=cut

sub getDBComment{
    my $self = shift;
    return $self->{'db_comment'};
}


=head2 B<_getAttributes()>

Abstract method. Has to be implemented in sub-classes

Returns a list of all attribute fields of object.

=cut

sub _getAttributes{
    my $self = shift;
    return ["id","confidence_level","accession","type","description","db_comment"];
}



1;
