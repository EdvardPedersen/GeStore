package GePan::PredictionTool;

use strict;

=head1 NAME

PredictionTool

=head1 DESCRIPTION

Super-class for data of a particular prediction tool a sequence was predicted with.

Stores all additional information of a sequence prediction that is not

stored in a simple sequence.

=head1 ATTRIBUTES

file = result file from prediction tool

input_file = file the prediction is based on

score = prediction score of this sequence

logger: GePan::Logger object

=head1 CONSTRUCTOR

=head2 B<new()>

Returns empty PredictionTool-object.

=cut

sub new{
    my $class = shift;
    my $self = {};
    return (bless($self,$class));
}

=head1 GETTER & SETTER METHODS

=head2 B<setFile(file)>

Sets the output file of the prediction.

=cut

sub setFile{
    my ($self,$file) = @_;
    $self->{'file'} = $file;
}

=head2 B<getFile()>

Returns path to the result file of the prediction.

=cut

sub getFile{
    my $self = shift;
    return $self->{'file'};
}

=head2 B<setInputFile(input_file)>

Sets path to the file the prediction was based on

=cut

sub setInputFile{
    my ($self,$file) = @_;
    $self->{'input_file'} = $file;
}

=head2 B<getInputFile()>

Returns path to the sequence file the prediction was based on

=cut

sub getInputFile{
    my $self = shift;
    return $self->{'original_file'};
}

=head2 B<setScore(score)>

Sets the glimmer3 score of this sequence.

=cut

sub setScore{
    my ($self,$score) = @_;
    $self->{'score'} = $score;
}

=head2 B<getScore()>

Returns the score of this sequence.

=cut

sub getScore{
    my $self = shift;
    return $self->{'score'};
}

=head2 B<setParams(ref)>

Sets all attributes of object by hash ref.

=cut

sub setParams{
    my ($self,$h) = @_;
    foreach(keys(%$h)){
	$self->{$_} = $h->{$_};
    }
}


=head2 B<getName()>

Returns name of the prediction tool used.

Note: Abstract method, has to be implemented in the sub-classes.

=cut

sub getName{
    my $self = shift;
    $self->LogError("Abstract method getName not implemented in sub-class.");
}

=head2 B<setLogger(GePan::Logger)>

Sets GePan::Logger object.

=cut

sub setLogger{
    my ($self,$l) = @_;
    $self->{'logger'} = $l;
}


1;
