package GePan::PredictionTool::Mga;
use base qw(GePan::PredictionTool);


=head1 NAME

GePan::PredictionTool::Mga

=head1 DESCRIPTION

Class for storing all gene-prediction relevant information of a sequence predicted by MetaGeneAnnotator.

=head1 ATTRIBUTES

complete = if start, stop both or nonw are missing in sequence

    11: sequence complete

    01: start is missing

    10: stop is missing

    11: start and stop are missing

model = which model was used for prediction

    s: sequence itself

    b: bacteria

    p: phage

    a: archaea

rbs_start = ribosomal start

rbs_stop = ribosomal stop

rbs_score = ribosomal score

=head1 GETTER & SETTER METHODS

=head2 B<setComplete(complete)>

Sets parameter complete to complete

=cut

sub setComplete{
    my ($self,$com) = @_;
    $self->{'complete'} = $com;
}

=head2 B<getComplete()>

Returns complete of sequence

=cut

sub getComplete{
    my $self = shift;
    return $self->{'complete'};
}

=head2 B<setModel()>

Sets used model of sequence

=cut

sub setModel{
    my ($self,$model) = @_;
    $self->{'model'} = $model;
}

=head2 B<getModel()>

Returns model sequence prediction was based on

=cut

sub getModel{
    my $self = shift;
    return $self->{'model'};
}

=head2 B<setRBSStart(rbs_start)>

Sets start of ribosome to rbs_start

=cut

sub setRBSStart{
    my ($self,$s) = @_;
    $self->{'rbs_start'} = $s;
}

=head2 B<getRBSStart()>

Returns risbosomal start point

=cut

sub getRBSStart{
    my $self = shift;
    return $self->{'rbs_start'};
}

=head2 B<setRBSStop(rbs_stop)>

Sets ribosomal stop of sequence/

=cut

sub setRBSStop{
    my ($self,$s) = @_;
    $self->{'rbs_stop'} = $s;
}

=head2 B<getRBSStop(rbs_stop)>

Returns ribosomal stop position.

=cut

sub getRBSStop{
    my $self = shift;
    return $self->{'rbs_stop'};
}

=head2 B<setRBSScore(rbs_score)>

Sets scroe of ribosomal docking position prediction

=cut

sub setRBSScore{
    my ($self,$s) = @_;
    $self->{'rbs_score'} = $s;
}

=head2 B<getRBSScore()>

Returns score of ribosomal docking prediction

=cut

sub getRBSScore{
    my $self = shift;
    return $self->{'rbs_score'};
}


=head2 B<getName()>

Returns the name of the prediction tool (glimmer3)

=cut

sub getName{
    return 'mga';
}

1;
