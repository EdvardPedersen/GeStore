package GePan::Sequence::Base::Predicted;

=head1 NAME

GePan::Sequence::Base::Predicted

=head1 DESCRIPTION

One of the basic sequence objects. All sequence types that are predicted by any kind of prediction software have to inherite from this class. 

=head1 ATTRIBUTES

parent_sequence: Name of sequence the prediction was made in (e.g. contig name)

prediction_tool: GePan::PredictionTool of tool the cds was predicted with

=head1 GETTER & SETTER METHODS

=head2 B<setParentSequence(string)>

Sets name of sequence the prediction was made in.

=cut

sub setParentSequence{
    my ($self,$p) = @_;
    $self->{'parent_sequence'} = $p;
}


=head2 B<getParentSequence()>

Returns name of sequence the prediction was based on.

=cut

sub getParentSequence{
    my $self = shift;
    return $self->{'parent_sequence'};
}   



=head2 B<setPredictionTool(GePan::PredictionTool)>

Sets prediction tool for this cds.

=cut

sub setPredictionTool{
    my ($self,$p) = @_;
    $self->{'prediction_tool'} = $p;
}

=head2 B<getPredictionTool()>

Returns GePan::PredictionTool of this object.

=cut

sub getPredictionTool{
    my $self = shift;
    return $self->{'prediction_tool'};
}


1;
