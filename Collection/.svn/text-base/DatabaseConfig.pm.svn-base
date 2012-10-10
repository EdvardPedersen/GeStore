package GePan::Collection::DatabaseConfig;
use base qw(GePan::Collection);

use strict;
use Data::Dumper;

=head1 NAME

GePan::Collection::DatabaseConfig

=head1 DESCRIPTION

Class for storing GePan::DatabaseConfig objects

=head1 GETTER & SETTER METHODS

=head2 B<getElementsByAttributeHash(ref)>

Returns GePan::Collection::Hit object with all hits that match given attribute keys and values.

=cut

sub getElementsByAttributeHash{
    my ($self,$h) = @_;
    my $collection = GePan::Collection::DatabaseConfig->new();
    $collection->setLogger($self->{'logger'});
    foreach my $db (@{$self->{'list'}}){
        my $true = 1;
        foreach my $key(keys(%$h)){
	    $true = ($db->{$key} eq $h->{$key})?$true:0;
	}
        $collection->addElement($db) unless !($true);
    }
    return $collection;
}


=head2 B<getDatabaseByFileName(string)>

Returns GePan::DatabaseConfig object of the database that matches a particular tool-output-file. Tool output files are named INPUT_FILE_NAME.DB_NAME.TOOL_NAME.out'

=cut

sub getDatabaseByFileName{
    my ($self,$name) = @_;
    my @a = ();

    while(my $db = $self->getNextElement()){
	my $dbName = $db->getID();
	push @a, $db unless ($name!~m/$dbName/i);
    }
    $self->{'logger'}->LogError("Collection::Database::getDatabaseByFileName() - More than one database matches given tool-output file.") unless scalar(@a)==1;
    $self->{'logger'}->LogError("Collection::Database::getDatabaseByFileName() - No database found matching given tool-output file.") unless scalar(@a);
    return $a[0];
}



1;

