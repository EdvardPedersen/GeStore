package GePan::ToolConfig::Prediction;
use base qw(GePan::ToolConfig);


use strict;
use Data::Dumper;

=head1 NAME

GePan::ToolConfig::Prediction

=head1 DESCRIPTION

Class to store ToolConfig attributes for all prediction tools.

=cut

=head1 GETTER & SETTER METHODS

=head2 B<setSubType(string)

Sets the type of the prediction tool.

=cut

sub setSubType{
    my ($self,$type) = @_;
    $self->{'sub_type'} = $type;
}

=head2 B<getSubType()>

Returns the sub-type of this prediction tool.

=cut

sub getSubType{
    my $self = shift;
    return $self->{'sub_type'};
}



1;
