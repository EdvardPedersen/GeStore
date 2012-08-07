package GePan::Collection::Hit;
use base qw(GePan::Collection);

use strict;
use Data::Dumper;

=head1 NAME

GePan::Collection::Hit

=head1 DESCRIPTION

Class for storing GePan::Hit objects

=head1 GETTER & SETTER METHODS

=head2 B<getSignificantHits()>

Returns hits with significant score, e-value etc

(See sub-class documentations for further information)

=cut

sub getSignificantHits{
    my $self = shift;
    my @s = grep{($_->_significant());} @{$self->{'list'}};
    return \@s;
}

=head2 B<getHitsByID(id)>

Returns list of all hits with given domain/gene id

=cut

sub getHitsByAnnotationID{
    my ($self,$id) = @_;
    return \(grep{($_->getID()) eq $id;} @{$self->{'list'}});
}

=head2 B<getHitsByDB(db_name)>

Returns all hits found in a database as array.

=cut

sub getHitsByDB{
    my ($self,$db) = @_;
    return \(grep{$_->{'database'} eq $db;} values(%{$self->{'list'}}));
}


=head2 B<getDBs()>

Returns list of database names of hits

=cut

sub getDBs{
    my $self = shift;
    my $result = {};
    foreach my $hit(@{$self->{'list'}}){
	$result->{$hit->{'database'}->{'name'}} = $hit->{'database'};
    }

    return $result;
}

=head2 B<getToolNames()>

Returns list of all tools hits in the collection are found by.

=cut

sub getToolNames{
    my $self = shift;
    my $names = {};
    foreach(@{$self->{'list'}}){
	$names->{$_->getToolName()} = 1;
    }
    my @t = keys(%$names);
    return \@t;
}

=head2 B<getElementsByAttributeHash(ref)>

Returns GePan::Collection::Hit object with all hits that match given attribute keys and values.

=cut

sub getElementsByAttributeHash{
    my ($self,$h) = @_;
    my $collection = GePan::Collection::Hit->new();
    $collection->setLogger($self->{'logger'});
    foreach my $hit (@{$self->{'list'}}){
        my $true = 1;
        foreach my $key(keys(%$h)){
	    ### Should be changed asap...
	    ### Bad bad bad bad programming style! 'database_format' is exception because it's actually $hit->{database}->{format}!
	    if($key eq "database_format"){
		$self->{'logger'}->LogError("Collection::Hit::getElementsByAttributeHash() - Key hit->{database}->{format} does not exist in hit database object.\n".(Dumper $hit)) unless exists $hit->{'database'}->{'format'};
		$true = 0 unless ($hit->{'database'}->{'format'} eq $h->{'database_format'});
	    }
	    else{	   
		$self->{'logger'}->LogError("Collection::Hit::getElementsByAttributeHash() -Key $key does not exist in hit-object.\n".(Dumper $hit)) unless exists $hit->{$key};
		### Should be changed asap...
		### Bad bad programming style! 'database' is exception because it's not a string but a hash-ref!
		if(ref $hit->{$key}){
		    if($key eq "database"){
			$true = 0 unless $hit->getDatabaseName() eq $h->{$key};	    
		    }
		}
		else{
		    $true = 0 unless $hit->{$key} eq $h->{$key};
		}
	    }
	}
        $collection->addElement($hit) unless !($true);
    }
    return $collection;
}


=head2 B<checkNonAnnotated()>

Checks if any of the hits in a collection has no annotation assigned.

Returns '1' as soon as it finds a hit without annotation.

=cut

sub checkNonAnnotated{
    my $self = shift;
    return 0 unless scalar(@{$self->{'list'}});
    foreach my $hit (@{$self->{'list'}}){
	return 1 unless $hit->{'annotation'};
    }   
    return 0;
}


=head2 B<getDatabases()>

Returns a list of names of all used databases

=cut

sub getDatabases{
    my $self = shift;
    my $dbs = {};
    foreach my $hit (@{$self->getList()}){
	$dbs->{$hit->getDatabaseName()} = 1;
    }
    return $dbs;
}


1;

