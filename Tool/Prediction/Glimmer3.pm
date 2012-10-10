package GePan::Tool::Prediction::Glimmer3;
use base qw(GePan::Tool);

use Data::Dumper;
use GePan::Config qw(GLIMMER3_PATH);
use GePan::Logger;
use strict;

=head1 NAME

GePan::Tool::Prediction::Glimmer3

=head1 DESCRIPTION

Class for executing Glimmer3 gene prediction tool.

Sub-class of GePan::Tool

=head1 METHODS

=head2 B<execute()>

Executes iterative glimmer3 script g3-iterated.csh

Output is written to given output dir.

=cut

sub execute{
    my $self = shift;

    

    my $statement =  $self->_getExecuteStatement();
    $self->{'logger'}->LogStatus("Glimmer execute statement: $statement");
    my $exit = system($statement);


    my @split = split(/\./,($self->{'output_file'}));
    my $last = pop(@split);
    my $tag = join(".",@split);
    if($last ne 'predict'){
        $tag .= ".$last";
    }


    my $outputPath = $self->{'output_dir'}.'/'.$self->{'output_file'};

    chdir($self->{'output_dir'});

    # move result file to result dir
    my $moveStatement = "mv ".$tag.".predict $outputPath";
    $self->{'logger'}->LogStatus("Move-statement = $moveStatement");
    $exit = system($moveStatement);
    $self->{'logger'}->LogError("No glimmer output file found.\n        Possible reasons:\n        - Glimmer3 didn't find any genes (wrong/too short input sequence?)\n        - glimmer3 didn't run (wrong glimmer path?)") if $exit;
    
    # remove everything that's not the result file
    opendir(DIR,$self->{'output_dir'});
    my @files = grep{-f $self->{'output_dir'}."/$_"}readdir(DIR);
    closedir(DIR);
    foreach(@files){

	warn "outp = $outputPath\nbla = ".$self->{'output_dir'}."/$_";

	if(($self->{'output_dir'}."/$_") ne $outputPath){
	    system("rm ".$self->{'output_dir'}."/$_");
	}
    }
}

=head2 B<getToolName()>

Returns short name of tool (\'glimmer\')

=cut

sub getToolName{
    my $self = shift;
    return "glimmer3";
}

=head1 INTERNAL METHODS


=head2 B<_getExecuteStatement()>

Creates and returns the execute statement for glimmer.

=cut

sub _getExecuteStatement{
    my $self = shift;

    if(!$self->{'output_file'}){
        $self->{'output_file'} = _createOutputName($self);
    }

    my @split = split(/\./,($self->{'output_file'}));
    my $last = pop(@split);
    my $tag = join(".",@split);
    if($last ne 'predict'){
        $tag .= ".$last";
    }

    my $statement = GLIMMER3_PATH."/g3-iterated.csh ".$self->{'input_file'}." $tag";

    return $statement;
}

=head2 B<_createOutputName()>

Creates name of output file.

output_file = glimmer_prediction.out.JOBID

=cut

sub _createOutputName{
    my $self = shift;
    my $name = "glimmer_prediction.out.".$self->{'job_id'};
    $self->{'output_file'} = $name.".predict";
}

1;
