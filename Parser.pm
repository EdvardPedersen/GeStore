package GePan::Parser;

use strict;
use GePan::Logger;

=head1 NAME

GePan::Parser

=head1 DESCRIPTION

Main-class of all parsers.

=head1 ATTRIBUTES

file = file to parse

logger: GePan::Logger object

=head1 CONSTRUCTOR

=head2 B<new()>

Returns an empty parser-object

=cut

sub new{
    my $class = shift;
    my $self = {file=>''};
    return (bless($self,$class));
}

=head1 GETTER AND SETTER METHODS

=head2 B<setParams(hash-ref)>

Sets all attributes by hash-ref of form { attribute_name => attribute_value }

=cut

sub setParams{
    my ($self,$h) = @_;
    foreach(keys(%$h)){
	$self->{$_} = $h->{$_};
    }
}

=head2 B<setFile(file)>

Sets path to file

=cut

sub setFile{
    my ($self,$file) = @_;
    $self->{'file'}= $file;
}

=head2 B<getFile()>

Returns path to file

=cut

sub getFile{
    my $self = shift;
    return $self->{'file'};
}

=head2 B<setLogger(GePan::Logger)>

Sets GePan::Logger object.

=cut

sub setLogger{
    my ($self,$l) = @_;
    $self->{'logger'} = $l;
}

=head2 B<getLogger()>

Returns GePan::Logger object.

=cut

sub getLogger{
    my $self = shift;
    return $self->{'logger'};
}


1;
