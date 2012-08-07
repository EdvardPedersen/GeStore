package GePan::Tool::Annotation::Krona;
use base qw(GePan::Tool);

use Data::Dumper;
use GePan::Config qw(KRONA_PATH);
use GePan::Logger;
use strict;

=head1 NAME

GePan::Tool::Annotation::Krona

=head1 DESCRIPTION

Class to start KronaTools, kt_taxonommy.

Sub-class of GePan::Tool

=head1 METHODS

=head2 B<execute()>


=cut

sub execute{
    my $self = shift;
#   $self->{'logger'}->LogWarning(Dumper($self));
    my $statement =  $self->_getExecuteStatement();
    $self->{'logger'}->LogStatus("Krona execute statement: $statement");
    
    my $exit = system($statement);

    my $outputPath = $self->{'output_dir'}.'/'.$self->{'output_file'};



}

=head2 B<getToolName()>

Returns short name of tool (\'null\')

=cut

sub getToolName{
    my $self = shift;
    return "krona";
}

=head1 INTERNAL METHODS


=head2 B<_getExecuteStatement()>

Creates and returns the execute statement for Megan.

=cut

sub _getExecuteStatement{
    my $self = shift;
    my $queueing = shift;
    #print STDOUT Dumper($self);
    my $paramscript = $self->createParameterScript($queueing);
    if(!$self->{'output_file'}){
	$self->{'output_file'} = _createOutputName($self);
    }
    $self->{'logger'}->{'status_log'} =~ /^(.*)logs\/GePan.log$/;
    my $workdir = $1;
    my $resultsdir = $workdir.'results/';
    my $statement = 'export PATH='.KRONA_PATH.':$PATH; '.'ktImportTaxonomy'.' '.$resultsdir.'Megan_Abundance.txt'.' -o '.$resultsdir.'Megan_Abundance.html'; 
    return $statement;
}

=head2 B<_createOutputName()>

Creates name of output file.

output_file = glimmer_prediction.out.JOBID

=cut

sub _createOutputName{
    my $self = shift;
    my $name = "megan.out.".$self->{'job_id'};
    $self->{'output_file'} = $name.'.predict';
}

=head2 B<createParameterScript>

Creates the second part of the MEGAN parameter file. This file is used to pipe commands to MEGAN in non-GUI mode and is stored in /shells.

=cut

sub createParameterScript{
    my $self = shift;
    my $queueing = shift;
    #Taking path from log, bad idea, fix this -emr
    $self->{'logger'}->{'status_log'} =~ /^(.*)logs\/GePan.log$/;
    my $workdir = $1;
    
    my $parameterfile = $workdir.'shells/MEGANparameters.txt';
    
    #Writes parameter script for MEGAN
    open(SCRIPT, ">>$parameterfile");
    
    print SCRIPT ' fastafile = '.$workdir.'data/read/nucleotide/input.fas ';
    print SCRIPT 'meganfile = '.$workdir.'tools/megan/megan.rma ';
    print SCRIPT 'minscore = 10 toppercent = 5 winscore = 0 minsupport = 2; ';
    print SCRIPT 'open file = '.$workdir.'tools/megan/megan.rma; update; ';
    print SCRIPT 'exportimage file = '.$workdir.'results/Megan_Results.pdf format = PDF replace = true; select nodes = all; export what = CSV format = taxonid_count separator = tab file = '.$workdir.'results/Megan_Abundance.txt; quit;';
    close SCRIPT;


    return $parameterfile;
}

1;
