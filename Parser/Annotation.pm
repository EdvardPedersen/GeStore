package GePan::Parser::Annotation;
use base qw(GePan::Parser);
use GePan::Logger;
use strict;


=head1 NAME

GePan::Parser:Annotation:

=head1 DESCRIPTION

Main-class of all annotation tool result parsers.

Sub-class of GePan::Parser

=head1 ATTRIBUTES

collection: GePan::Collection::Hit object

database: GePan::Databases object

=head1 GETTER AND SETTER METHODS

=head2 B<getDB()>

Returns database hash of object

=cut

sub getDB{
    my $self = shift;
    return $self->{'database'};
}


=head2 B<setDB(GePan::Databases)

Set GePan::Databases object.

=cut

sub setDB{
    my ($self,$db) = @_;
    $self->{'database'} = $db;
}

=head2 B<getCollection()>

Returns collection of parser. 

=cut

sub getCollection{
    my $self = shift;
    return $self->{'collection'};
}


=head1 INTERNAL METHODS

=head2 _parseFile()

Abstract method. Has to be implemented by sub-classes

=cut

sub parseFile{
    my $self = shift;
    $self->{'logger'}->LogError("Abstract method _parseFile() not implemented.");
}


1;
