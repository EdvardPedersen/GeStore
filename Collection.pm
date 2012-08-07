package GePan::Collection;

use strict;
use Data::Dumper;
use GePan::Logger;

=head1 NAME

GePan::Collection

=head1 DESCRIPTION

Base class for collection of single objects, e.g. annotations or sequences

=head1 ATTRIBUTES

list: list of all elements in collection

logger: GePan::Logger object

=head1 CONSTRUCTOR

=head2 B<new()>

Returns an empty GePan::Collection object

=cut

sub new{
    my $class = shift;
    my $self = {list=>[]};
    return (bless($self,$class));
}

=head1 METHODS

=head2 B<addElement(element)>

Adds one elements to the list of a collection

=cut

sub addElement{
    my ($self,$e) = @_;
    push @{$self->{'list'}},$e;
} 

=head1 GETTER & SETTER METHODS

=head2 B<getSize()>

Returns number of elements in collection.

=cut

sub getSize{
    my $self=  shift;
    if(scalar(@{$self->{'list'}})){
	return scalar(@{$self->{'list'}});
    }
    else{
	return 0;
    }
}

=head2 B<setList(ref)>

Sets list to array ref.

=cut

sub setList{
    my ($self,$l) = @_;
    $self->{'list'} = $l;
}

=head2 B<getList()>

Retusn list of elements.

=cut

sub getList{
    my $self=  shift;
    return $self->{'list'};
}

=head2 B<getElementByID(id)>

Returns element by id.

=cut

sub getElementByID{
    my ($self,$id) = @_;
    $self->{'logger'}->LogError("Collection::getElementByID() - No ID given in getElementByID().") unless $id;
    foreach(@{$self->{'list'}}){
	if($_->getID() eq $id){
	    return $_;
	}
    }
    return 0;
}


=head2 <getElementsByAttributeHash(ref)>

Abstract method. Has to be implemented by sub-class.

Returns GePan::Collection::SUB-CLASS containing all elements that match the given attributes.

Attribtues given by hash-ref of form { attribute_name=>attribute_value}

=cut

sub getElementsByAttributeHash{
    my $self = shift;
    $self->{'logger'}->LogError("Collection::getElementsByAttributehash() -  Abstract method getElementByAttributeHash() not implemented in sub-class.");
}


=head2 B<getNextElement()>

Returns all elements of collection one by one.
Returns 0 when all elements are returned.
Starts at element 0 of the list if called again.

=cut

sub getNextElement{
    my $self = shift;
    if($self->{'elementCount'}){
        if(($self->{'elementCount'})<scalar(@{$self->{'list'}})){
            $self->{'elementCount'} = $self->{'elementCount'}+1;
            return @{$self->{'list'}}[($self->{'elementCount'})-1];
        }
        else{
            $self->{'elementCount'} = undef;
            return 0;
        }
    }
    else{
        $self->{'elementCount'} = 1;
        return @{$self->{'list'}}[0];
    }
}


=head2 B<setParams(ref)>

Given a hash-ref sets self->{hash_key} to the corresponding values.

=cut

sub setParams{
    my ($self,$params) = @_;
    foreach(keys(%$params)){
	$self->{$_} = $params->{$_};
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

