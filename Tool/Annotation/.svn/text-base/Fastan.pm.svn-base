package GePan::Tool::Annotation::Fastan;
use base qw(GePan::Tool::Annotation);

use strict;
use GePan::Config qw(FASTA_PATH DATABASE_PATH);

=head1 NAME

GePan::Tool::Annotation::FASTA

=head1 DESCRIPTION

Class for calling fasta-programs..

Sub-class of GePan::Tool

Fasta call:

    program -O output_file -L -m 9 -H -B -b 20 input_file DATABASE 

    where DATABASE = either database name or database list file

=head1 ATTRIBUTES

=head1 METHODS

=head2 B<execute()>

Starts a fasta_run with given multiple fasta and database

=cut

sub execute{
    my $self = shift;
    
    my $statement = $self->_getExecuteStatement();
    $self->{'logger'}->LogStatus("FASTA call: $statement");
    my $exit = system($statement);
    if($exit){
	$self->{'logger'}->LogError("\n[ERROR] fasta exit(".($exit/256).")\n");
    }
}

=head1 GETTER & SETTER METHODS

=head2 B<getToolName()>

Returns string 'fasta'

=cut

sub getToolName{
    return "fasta";
}

=head2 B<setProgram()>

Sets fasta program to run

=cut

sub setProgram{
    my ($self,$p) = @_;
    $self->{'program'} = $p;
}

=head2 B<getProgram()>

Returns name of fasta program.

=cut

sub getProgram{
    my $self = shift;
    return $self->{'program'};
}


=head1 INTERNAL METHODS


=head2 B<_getExecuteStatement()>

Creates and returns the execute statement for glimmer.

=cut

sub _getExecuteStatement{
    my $self = shift;
    
    my $outputdir = $self->{'output_dir'};
    my $outputFile = $self->{'output_file'};
    my $outputComplete = $outputdir."/".$outputFile;
    $outputComplete=~s/\/\//\//g;

    my $db_path = $self->{'database'}->getPath();
    $db_path=~s/\/\//\//g;

    my $fastaExec;

    my $tmp = FASTA_PATH;
    $tmp=~s/\/\//\//g;
    $fastaExec=$tmp;

    my $statement = "$fastaExec -L -m 9 -H -B -b 5 \"".$self->{'input_file'}."\" @".$db_path.".list > $outputComplete";
    
    return $statement;
}

1;
