package GePan::Tool::Annotation::Blast;
use base qw(GePan::Tool::Annotation);

use strict;
use GePan::Config qw(BLAST_PATH DATABASE_PATH);
use GePan::Logger;
use Data::Dumper;


=head1 NAME

GePan::Tool::Annotation::Blast

=head1 DESCRIPTION

Class for calling Blast.

Sub-class of GePan::Tool

Blast statement:

    blastall -p program -d database -i input_file -o output_file -b 20 -m7

=head1 ATTRIBUTES

program :  kind of blast (e.g. blastp, blastn)

cpu : number of CPUs used (in stand-alone version)

=head1 METHODS

=head2 B<execute()>

Starts a blast_run with given multiple fasta, database and program type

=cut

sub execute{
    my $self = shift;

    my $statement = $self->_getExecuteStatement();

    if($self->getCPU()){
	$statement.= "-a ".$self->getCPU()." ";
    }
    $self->{'logger'}->LogStatus("Blast-call: $statement");
    my $exit = system($statement);
    if($exit){
	$self->{'logger'}->LogError("blast exit(".($exit/256).")\n");
    }
}

=head1 GETTER & SETTER METHODS

=head2 B<setProgram(program)>

Sets the program type for blast run.

possible types are blastp and blastx. No nucleotide databases supported yet.

=cut

sub setProgram{
    my ($self,$p) = @_;
    $self->{'program'} = $p;
}

=head2 B<getProgram()>

Returns type of program used for this blast object.

=cut

sub getProgram{
    my $self = shift;
    return $self->{'program'};
}

=head2 B<getToolName()>

Returns string "blast"

=cut

sub getToolName{
    return "blast";
}

=head2 B<setCPU(int)>

Sets number of CPUs used for this tool (for stand-alone version only)

=cut

sub setCPU{
    my ($self,$int) = @_;
    $self->{'cpu'} = $int;
}

=head2 B<getCPU()>

Returns number of CPUs used (for stand-alone version only)

=cut

sub getCPU{
    my $self = shift;
    return $self->{'cpu'};
}

=head1 INTERNAL METHODS


=head2 B<_getExecuteStatement()>

Creates and returns the execute statement for glimmer.

=cut

sub _getExecuteStatement{
    my $self = shift;
# Create first part of Meganscript?
    my $queueing = shift;
    $self->createParameterScript($queueing);
#End of changes
    my $outputdir = $self->{'output_dir'};
    my $outputFile = $self->{'output_file'};
    my $outputComplete = $outputdir."/".$outputFile;
    $outputComplete=~s/\/\//\//g;
    my $db_path = $self->{'database'}->getPath();
    $db_path=~s/\/\//\//g;
    my $statement = BLAST_PATH." -p ".$self->{'program'}." -d $db_path -i ".$self->{'input_file'}." -o $outputComplete -b 5 -m 7 ";
    return $statement;
}

=head2 B<createParameterScript()>
Creates the first section of the MEGAN parameter file
=cut
sub createParameterScript{
    my $self = shift;
    my $queueing = shift;
    $self->{'logger'}->{'status_log'} =~ /^(.*)logs\/GePan.log$/;
    my $workdir = $1;
    my $parameterfile = $workdir.'shells/MEGANparameters.txt';
    $self->{'output_file'} =~ /^(.*)\.\$SGE_TASK_ID$/;
    my $outputfile = $1;
    my $blastfile = $workdir.'tools/blastn/'.$outputfile;
    my @blastfiles;
    for (my $count=1; $count<=$queueing; $count++){
        push(@blastfiles, $blastfile.'.'.$count);
    }
    #Writes parameter script for MEGAN
    open(SCRIPT, ">$parameterfile");
    print SCRIPT 'import blastfile = ';
    foreach (@blastfiles){
        print SCRIPT $_;
        if($_ eq $blastfiles[-1]){next;}
        print SCRIPT ' ,';
    }
    close SCRIPT;

}

1;
