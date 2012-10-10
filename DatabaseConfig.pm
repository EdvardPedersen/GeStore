package GePan::DatabaseConfig;

use strict;
use Data::Dumper;
use GePan::Config qw(DATABASE_PATH);
use GePan::Logger;

=head1 NAME

GePan::DatabaseConfig

=head1 DESCRIPTION

Class for representation of one database.

=head1 ATTRIBUTES

id: name of the database

path: path to the database files

sequence_type: type of sequences in the database (protein or nucleotide)

database_format: format of the database, e.g. blast or pfam

database_taxon: taxon of included sequences, e.g. virus

logger: GePan::Logger object

=head1 CONSTRUCTOR

=head2 B<new()>

Returns an empty GePan::Database object;

=cut

sub new{
    my $class = shift;
    my $self = {};
    return (bless($self,$class));
}


=head1 GETTER & SETTER METHODS

=head2 B<setID(string)>

Sets name of the database.

=cut

sub setID{
    my ($self,$name) = @_;
    $self->{'name'} = $name;
}


=head2 B<getID()>

Returns  name of the database.

=cut

sub getID{
    my $self = shift;
    return $self->{'name'};
}

=head2 B<setPath(path)>

Sets path to the database file(s).

=cut

sub setPath{
    my ($self,$p) = @_;
    $self->{'path'} = $p;
}

=head2 B<getPath()>

Returns path to the database file(s).

=cut

sub getPath{
    my $self = shift;
    return $self->{'path'};
}

=head2  B<setSequenceType(type)>

Sets type of the sequences included in the database, e.g. protein

=cut

sub setSequenceType{
    my ($self,$t) = @_;
    $self->{'sequence_type'} = $t;
}

=head2 B<getSequenceType()>

Returns type of the sequences included in the database, e,g, protein.

=cut

sub getSequenceType{
    my $self = shift;
    return $self->{'sequence_type'};
}

=head2 B<setDatabaseFormat(format)>

Sets format of database, e.g. blast or pfam.

=cut

sub setDatabaseFormat{
    my ($self,$f) = @_;
    $self->{'database_format'} = $f;
}

=head2 B<getDatabaseFormat()>

Returns format of the database, e.g. blast or pfam.

=cut

sub getDatabaseFormat{
    my $self = shift;
    return $self->{'database_format'};
}

=head2 B<setDatabaseTaxon(taxon)>

Sets taxon of sequences included in database, e.g. virus, vertebrates or all.

=cut

sub setDatabaseTaxon{
    my ($self,$t) = @_;
    $self->{'database_taxon'} = $t;
}

=head2 B<getDatabaseTaxon()>

Returns taxon of sequences included in database, e.g. virus, vertebrates or all.

=cut

sub getDatabaseTaxon{
    my $self = shift;
    return $self->{'database_taxon'};
}

=head2 B<setParams(ref)>

Sets all keys of given hash as parameter to given value.

=cut

sub setParams{
    my ($self,$h) = @_;
    foreach(keys(%$h)){
        $self->{$_} = $h->{$_};
    }
}

=head2 B<setLogger(GePan::Logger)>

Sets GePan::Logger object.

=cut

sub setLogger{
    my ($self,$l) = @_;
    $self->{'logger'} = $l;
}


1;
