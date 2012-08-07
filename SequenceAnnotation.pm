package GePan::SequenceAnnotation;
use strict;
use GePan::Collection::Hit;

=head1 NAME

GePan::SequenceAnnotation

=head1 DESCRIPTION

Class for storing the final annotation of a sequence. Stores functional and transferred  annotation

id:  name of sequence annotated.

functional: hit the functional annotation is based on.

transferred: hit the  transferred annotation is based on.

confidence_level: final confidence level of annotation

attribute_collection: GePan::Collection::Hit object of hits of attribute prediction tools

=head1 CONSTRUCTOR

=head2 B<new()>

Returns empty GePan::SequenceAnnotation object

=cut

sub new{
    my $class = shift;
    my $self = {attribute_collection=>GePan::Collection::Hit->new()};
    return (bless($self,$class));
}

=head1 GETTER & SETTER METHODS

=head2 B<setFunctionalAnnotation(GePan::Hit::Pfam)>

Sets the hit including annotation the functional annotation for this object is based on;

=cut

sub setFunctionalAnnotation{
    my ($self,$a) = @_;
    $self->{'functional'} = $a;
}


=head2 B<getFunctionalAnnotation()>

Returns GePan::Hit::Pfam if available. 0 otherwise

=cut

sub getFunctionalAnnotation{
    my $self = shift;
    if($self->{'functional'}){
	return $self->{'functional'};
    }
    else{
	return 0;
    }
}

=head2 B<setTransferrred(GePan::Hit::XXX)>

Sets hit including annotation the transferred annotation for this object is based on

=cut

sub setTransferredAnnotation{
    my ($self,$a) = @_;
    $self->{'transferred'} = $a;
}

=head2 B<getTransferredAnnotation()>

Returns GePan::Hit::XXX object of the transferred annotation

=cut
sub getTransferredAnnotation{
    my $self = shift;
    return $self->{'transferred'};
}

=head2 B<setConfidenceLevel(int)>

Sets confidence level of annotation.

=cut

sub setConfidenceLevel{
    my ($self,$int) = @_;
    $self->{'confidence_level'} = $int;
}

=head2 B<getConfidenceLevel()>

Returns confidence level of annotation.

=cut

sub getConfidenceLevel{
    my $self = shift;
    return $self->{'confidence_level'};
}

=head2 B<setAttributeCollection(GePan::Collection::Hit)>

Sets GePan::Collection::Hit object of hits found by attribute prediction tools.

=cut

sub setAttributeCollection{
    my ($self,$c) = @_;
    $self->{'attribute_collection'} = $c;
}

=head2 B<getAttributeCollection()>

Returns GePan::Collection::Hit object of hits found by attribute prediction tools.

=cut

sub getAttributeCollection{
    my $self = shift;
    return $self->{'attribute_collection'};
}


1;
