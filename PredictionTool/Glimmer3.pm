package GePan::PredictionTool::Glimmer3;
use base qw(GePan::PredictionTool);
=head1 NAME

GePan::PredictionTool::Glimmer3

=head1 DESCRIPTION

Class for storing all gene-prediction relevant information of a sequence, i.e. score or strand.

=head1 GETTER & SETTER METHODS

=head2 B<getName()>

Returns the name of the prediction tool (glimmer3)

=cut

sub getName{
    return 'glimmer3';
}

1;
