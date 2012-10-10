package GePan::Sequence;

=head1 NAME

GePan::Sequence

=head1 DESCRIPTION

Base-class for all sequence objects, nucleotide or protein.

=head1 ATTRIBUTES 

start: start of sequence

stop: stop of sequence

sequence: sequence of object

sequence_type: type of sequence

id: name of sequence

logger: GePan::Logger object

=cut 

use strict;
use GePan::Logger;
use Data::Dumper;

=head1 CONSTRUCTORS

=head2 B<new()>

Creates an (empty) Sequence object

=cut

sub new{
    my $class = shift;
    my $self =	{sequence=>'',
		id=>'',
		start=>'',
		stop=>''};
    return(bless($self,$class));
}


=head1 METHODS

=head1 GETTER & SETTER METHODS


=head2 B<setParams(hash-ref)>

Sets all parameter by hash of form
{ parameter_name => parameter_value }

=cut

sub setParams{
    my ($self,$params) = @_;
    foreach(keys(%$params)){
	$self->{$_} = $params->{$_};
    }
}


=head2 B<setID(id)>

Sets name of sequence to name.

=cut

sub setID{
    my ($self,$name) = @_;
    $self->{'id'} = $name;
}

=head2 B<getID()>

Returns name of sequence.

=cut

sub getID{
    my $self = shift;
    return $self->{'id'};
}

=head2 B<setStart(start)>

Sets start of sequence to start

=cut

sub setStart{
    my ($self,$start) = @_;
    $self->{'start'} = $start;
}

=head2 B<getStart()>

Returns start of sequence.

=cut

sub getStart{
    my $self = shift;
    return $self->{'start'};
}

=head2 B<setStop(stop)>

Sets stop of sequence to stop.

=cut;

sub setStop{
    my ($self,$stop) = @_;
    $self->{'stop'} = $stop;
}

=head2 B<getStop()>

Returns stop of sequence.

=cut

sub getStop{
    my $self = shift;
    return $self->{'stop'};
}


=head2 B<setSequence(sequence)>

Sets the sequence of this sequence object

=cut

sub setSequence{
    my ($self,$seq) = @_;
    $self->{'sequence'} = $seq;
}

=head2 B<getSequence()>

Returns sequence of object.

=cut

sub getSequence{
    my $self = shift;
    return $self->{'sequence'};
}


=head2 B<getLength()>

Returns sequence length.

=cut

sub getLength{
    my $self = shift;

    if($self->{'sequence'}){
	return length($self->{'sequence'});
    }
    elsif(($self->{'start'})&&($self->{'stop'})){
	if($self->{'start'}<$self->{'stop'}){
	    return ($self->{'stop'}-($self->{'start'}-1));
	}
	else{
	    return ($self->{'start'}-($self->{'stop'}-1));
	}
    }
    else{
	$self->{'logger'}->LogError("Failed to calculate sequence length: neither sequence nor start and stop set.");
    }
}

=head2 B<setLogger(GePan::Logger)>

Sets GePan::Logger object.

=cut

sub setLogger{
    my ($self,$l) = @_;
    $self->{'logger'} = $l;
}


=head1 ABSTRACT METHODS

=head2 B<getSequenceType()>

Returns sequence type of object, e.g. cds, contig etc

=cut

sub getSequenceType{
    my $self = shift;
    $self->{'logger'}->LogError("Abstract method getSequenceType() not implemented.");
}


1;
