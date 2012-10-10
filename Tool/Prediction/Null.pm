package GePan::Tool::Prediction::Null;
use base qw(GePan::Tool);

use Data::Dumper;
use GePan::Logger;
use strict;

=head1 NAME

GePan::Tool::Prediction::Null

=head1 DESCRIPTION

Class for Null, bypasing prediction and passing sequnces directly to $Collection

Sub-class of GePan::Tool

=head1 METHODS

=head2 B<execute()>

Does not execute. Copies input files to output to bypass prediction.

=cut

sub execute{
    my $self = shift;

    

    my $statement =  $self->_getExecuteStatement();
    $self->{'logger'}->LogStatus("Prediction execute statement: $statement");
    my $exit = system($statement);

    my $outputPath = $self->{'output_dir'}.'/'.$self->{'output_file'};






}

=head2 B<getToolName()>

Returns short name of tool (\'null\')

=cut

sub getToolName{
    my $self = shift;
    return "null";
}

=head1 INTERNAL METHODS


=head2 B<_getExecuteStatement()>

Creates and returns the execute statement for Null-Pred.

=cut

sub _getExecuteStatement{
    my $self = shift;

    if(!$self->{'output_file'}){
	$self->{'output_file'} = _createOutputName($self);
    }
    my $statement = 'cp '.$self->{'input_file'}.' '.$self->{'output_file'}.'.predict'; 
    return $statement;
}

=head2 B<_createOutputName()>

Creates name of output file.

output_file = glimmer_prediction.out.JOBID

=cut

sub _createOutputName{
    my $self = shift;
    my $name = "null.out.".$self->{'job_id'};
    $self->{'output_file'} = $name.'.predict';
}

1;
