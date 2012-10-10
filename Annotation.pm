package GePan::Annotation;

use strict;

=head1 NAME

GePan::Annotation

=head1 DESCRIPTION

Base class for all annotation object, e.g. pfam or blast annotations.

=head1 ATTRIBUTES

id: main identifier of pfam class or uniprot sequence

description: description of gene/domain/family

functional: 1 if annotation found in a functional database (e.g. pfam), 0 otherwise

confidence_level : confidence level of hit

logger: GePan::Logger object

=head1 CONSTRUCTOR

=head2 B<new()>

Returns an empty GePan::Annotation object

=cut

sub new{
    my $class = shift;
    my $self = {};
    return (bless($self,$class));
}

=head1 GETTER & SETTER METHODS

head2 B<setConfidenceLevel(confidence_level)>

Sets confidence level for this annotation

=cut

sub setConfidenceLevel{
    my ($self,$c) = @_;
    $self->{'confidence_level'} = $c;
}

=head2 B<getConfidenceLevel()>

Returns confidence level for this annotation

=cut

sub getConfidenceLevel{
    my $self= shift;
    return $self->{'confidence_level'};
}

=head2 B<setParams(ref)>

Sets all attributes of annotation object by hash-ref { attribute_name => attribute_value }

=cut

sub setParams{
    my ($self,$h) = @_;
    foreach(keys(%$h)){
	$self->{$_} = $h->{$_};
    }
}



=head2 B<setID(identifier)>

Sets identifier of object.

=cut

sub setID{
    my ($self,$id) = @_;
    $self->{'id'} = $id;
}

=head2 B<getID()>

Returns identifier of annotation object;

=cut

sub getID{
    my $self = shift;
    return $self->{'id'};
}

=head2 B<setDescription(description)>

Sets description of annotation.

=cut

sub setDescription{
    my ($self,$desc) = @_;
    $self->{'description'} = $desc;
}

=head2 B<getDescription()>

Returns description of annotation object.

=cut

sub getDescription{
    my $self = shift;
    return $self->{'description'};
}

=head2 B<setLogger(GePan::Logger)>

Sets GePan::Logger object.

=cut

sub setLogger{
    my ($self,$l) = @_;
    $self->{'logger'} = $l;
}


=head2 B<_getAttributes()>

Abstract method. Has to be implemented in sub-classes

Returns a list of all attribute fields of object.

=cut

sub _getAttributes{
    my $self = shift;
    $self->{'logger'}->LogError("GePan::Hit - Abstract method \'_getAttributes\' not implemented in sub-class.");
}



1;
