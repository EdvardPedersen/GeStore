package GePan::ToolConfig::Annotation;
use base qw(GePan::ToolConfig);


use strict;
use Data::Dumper;

=head1 NAME

GePan::ToolConfig::Annotation

=head1 DESCRIPTION

Class to store ToolConfig attributes for all annotation tools.

=head1 ATTRIBUTES   

sub_type: Type of annotation tool. Possible values: transferred,functional or structural

db_format: Format of the database the tool is run on, e.g. blast or hmm. 

db_type: type of sequences inlcuded in database, e.g. protein

=cut

=head1 GETTER & SETTER METHODS

=head2 B<setSubType(string)

Sets the type of the annotation tool.

=cut

sub setSubType{
    my ($self,$type) = @_;
    $self->{'sub_type'} = $type;
}

=head2 B<getSubType()>

Returns the sub-type of this annotation tool.

=cut

sub getSubType{
    my $self = shift;
    return $self->{'sub_type'};
}


=head2 B<setDBType(type)>

Sets database type, e.g. nucleotide, for given tool.

=cut

sub setDBType{
    my ($self,$type) = @_;
    $self->{'db_type'} = $type;
}


=head2 B<getDBType()>

Returns database type of tool.

=cut

sub getDBType{
    my $self = shift;
    return $self->{'db_type'};
}


=head2 B<setDBFormat(string)

Sets the format the database this tool runs on has to be formated in.

=cut


sub setDBFormat{
    my ($self,$format) = @_;
    $self->{'db_format'} = $format;
}


=head2 B<getDBFormat()>

Returns the format the database this tool runs on has to be formated in.

=cut

sub getDBFormat{
    my $self = shift;
    return $self->{'db_format'};
}


1;
