package GePan::Hit;
use strict;
use Data::Dumper;
use GePan::Logger;
=head1 NAME

GePan::Hit

=head1 DESCRIPTION

Super-class of all hits found by any tool

=head1 ATTRIBUTES

tool: name of tool the hit was found by

file: result file of tool the hit was found in

query_name: name of the query sequence

length : length of matching hit

id: name of the matching sequence/family

database: database the hit was found in

annotation: GePan::Annotation object for hit

score: score of hit.

e_value: e_value of hit. 

significance: 0/1. Determines whether the hit is significant or not.

     (See internal method _significant() of sub-class for further information)

logger: GePan::Logger object

=head1 CONSTRUCTOR

=head1 B<new()>

Returns an empty Parser::Anotation::Hit object

=cut

sub new{
    my $class = shift;
    my $self = {};
    return (bless($self,$class));
}

=head1 GETTER & SETTER METHODS

=head2 B<setParams(hash-ref)>

Sets all attributes of object by hash-ref of form { attribute_name = >attribute_value }

=cut

sub setParams{
    my ($self,$p) = @_;
    foreach(keys(%$p)){
	if($_ eq 'annotation'){
	    $self->setAnnotation($p->{$_});
	}
	else{
	    $self->{$_} = $p->{$_};
	}
    }
}


=head2 B<setScore(score)>

Sets score of hit. Score:

    Blast = average scrore of all HSPs

    Fastsa = z-score

    Pfam = family or domain bit-score

=cut

sub setScore{
    my ($self,$s) = @_;
    $self->{'score'} = $s;
}

=head2 B<getScore()>

Returns score of hit.

=cut

sub getScore{
    my $self = shift;
    return $self->{'score'};
}

=head2 B<setEValue(e_value)>

Sets e_Value of hit.

=cut

sub setEValue{
    my ($self,$e) = @_;
    $self->{'e_value'} = $e;
}

=head2 B<getEValue()>

Returns e-value of hit.

=cut

sub getEValue{
    my $self = shift;
    return $self->{'e_value'};
}

=head2 <setFile(file)>

Sets the file to parse

=cut

sub setFile{
    my ($self,$file) = @_;
    $self->{'file'} = $file;
}

=head2 B<getFile()>

Returns path to parsed file

=cut

sub getFile{
    my $self = shift;
    return $self->{'file'};
}

=head2 B<setQueryName(query_name)>

Sets name of the query sequence

=cut

sub setQueryName{
    my ($self,$name) = @_;
    $self->{'query_name'} = $name;
}

=head2 B<getQueryName()>

Returns name of the query sequence

=cut

sub getQueryName{
    my $self = shift;
    return $self->{'query_name'};
}

=head2 B<setID(id)>

Sets id of the matching sequence/family

=cut

sub setID{
    my ($self,$id) = @_;
    $self->{'id'} = $id;
}

=head2 B<getID()>

Returns id of the matching sequence/family

=cut

sub getID{
    my $self = shift;
    return $self->{'id'};
}

=head2 B<setAnnotation(GePan::Annotation)

Sets annotation object of hit.

Calls _significant().

=cut

sub setAnnotation{
    my ($self,$a) = @_;
    $self->{'annotation'} = $a;
    $self->_significant();
}

=head2 B<getAnnotation()>

Returns annotation object of this hit.

=cut

sub getAnnotation{
    my $self = shift;	
    return $self->{'annotation'};
}

=head2 B<getSignificance()>

Returns 1 for a significant hit, 0 otherwise

For further details on how the significance is evaluated see internal method _significant of sub-classes

=cut

sub getSignificance{
    my $self = shift;
    return $self->{'significance'};
}

=head2 B<getDatabaseName()>

Returns database name the hit was found in

=cut

sub getDatabaseName{
    my $self = shift;
    return $self->{'database'}->{'name'};
}

=head2 B<getDB()>

Returns DatabaseConfig object of database the hit was found in.

=cut

sub getDB{
    my $self = shift;
    return $self->{'database'};
}

=head2 B<setLength()>

Sets length of hit (either sequence or domain length)

=cut

sub setLength{
    my ($self,$l) = @_;
    $self->{'length'} = $l;
}

=head2 B<getLength()>

Returns length of the hit

=cut

sub getLength{
    my $self = shift;
    return $self->{'length'};
}


=head2 B<setLogger(GePan::Logger)>

Sets GePan::Logger object.

=cut

sub setLogger{
    my ($self,$l) = @_;
    $self->{'logger'} = $l;
}




=head1 INTERNAL METHODS

=head2 B<_significant()>

Abstract method. Has to be implemented in sub-class!

=cut

sub _significant{
    my $self = shift;
    $self->{'logger'}->LogError("Abstract method \'_significant()\' not implemented.");
}

=head2 B<getToolName()>

Returns the name of the tool the hit was found by.

=cut

sub getToolName{
    my $self = shift;
    return $_->{'tool'};
}


=head2 B<_getAttributes()>

Abstract method. Has to be implemented in sub-classes

Returns a list of all attribute fields of object.

=cut

sub _getAttributes{
    my $self = shift;
    $self->{'logger'}->LogError("GePan::Hit - Abstract method \'_getAttributes\' not implemented in sub-class.");
}

=head2 B<getName()>

Abstract method has to be implemented in sub-class (attribute hits)

=cut

sub getName{
    my $self = shift;
    $self->{'logger'}->LogError("GePan::Hit - Abstract method 'getName' not implemented in sub-class.");
}

1;
