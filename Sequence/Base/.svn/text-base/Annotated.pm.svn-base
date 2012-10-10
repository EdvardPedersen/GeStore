package GePan::Sequence::Base::Annotated;

=head1 NAME

GePan::Sequence::Base::Annotated;

=head1 DESCRIPTION

One of the basic sequence objects. All sequence types that might have a annotation have to inherite from this class. 

=head1 ATTRIBUTES

annotation:  GePan::Annotation object of this sequence.

=head1 GETTER & SETTER METHODS

=head2 B<setAnnotation(GePan::Annotation)>

Sets annotation of the cds.

=cut

sub setAnnotation{
    my ($self,$a) = @_;
    $self->{'annotation'} = $a;
}


=head2 B<getAnnotation()>

Returns GePan::Annotation object of this cds.

=cut

sub getAnnotation{
    my $self = shift;
    if($self->{'annotation'}){
	return $self->{'annotation'};
    }
    else{
	return 0;
    }
}


1;
