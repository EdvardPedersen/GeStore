package GePan::Tool;
use strict;
use GePan::Logger;
=head1 NAME

GePan::Tool

=head1 DESCRIPTION

Super-class for all tools, prediction as well as annotation tools. 

=head1 ATTRIBUTES

input_file = input file for tool

output_dir = output directory of result files of tool

output_file = name of output file of tool

job_id = jobID or job_arrayID of job

parameter = hash-ref of all parameter the tool should be started with. 

logger: GePan::Logger object

cpu: number of cpus used

=head1 CONSTRUCTOR

=head1 B<new()>

Returns an empty GePan::Tool object

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
	$self->{$_} = $p->{$_};
    }
}


=head2 <setInputFile(file)>

Sets the file input file for the tool 

=cut

sub setFile{
    my ($self,$file) = @_;
    $self->{'input_file'} = $file;
}

=head2 B<getInputFile()>

Returns path to input file

=cut

sub getFile{
    my $self = shift;
    return $self->{'input_file'};
}

=head2 B<setOutputDir>

Sets path to output directory for result files

=cut

sub setOutputDir{
    my ($self,$name) = @_;
    $self->{'output_dir'} = $name;
}

=head2 B<getOutputDir>

Returns path to directory for result files of tool

=cut

sub getOutputDir{
    my $self = shift;
    return $self->{'output_dir'};
}

=head2 B<setJobID(jobID)>

Sets job-id or job-arrayID 

=cut

sub setJobID{
    my ($self,$id) = @_;
    $self->{'job_id'} = $id;
}

=head2 B<getJobID>

Returns job-id or job-arrayID of tool

=cut

sub getJobID{
    my $self = shift;
    return $self->{'job_id'};
}


=head2 B<setOutputFile(output_file)>

Sets name of output file of tool.

=cut

sub setOutputFile{
    my ($self,$name) = @_;
    $self->{'output_file'} = $name;
}

=head2 B<getOutputFile()>

Returns name of output file of tool

=cut

sub getOutputFile{
    my $self = shift;
    return $self->{'output_file'};
}


=head2 B<getCPU()>

Returns number of cpus available.

=cut

sub getPU{
    my $self = shift;
    return $self->{'cpu'};
}

=head2 <setParameter(hash-ref)>

Sets number of cpus available.

=cut

sub setCPU{
    my ($self,$h) = @_;
    $self->{'cpu'} = $h;
}



=head2 B<getParameter()>

Returns a hash ref of all given tool parameter.

=cut

sub getParameter{
    my $self = shift;
    return $self->{'parameter'};
}

=head2 <setParameter(hash-ref)>

Sets a hash-ref of all parameter the tool should be called with.

=cut

sub setParameter{
    my ($self,$h) = @_;
    $self->{'parameter'} = $h;
}

=head2 B<getParameterString()>

Returns a string of all additional parameter the tool shoud be called with. 

=cut

sub getParameterString{
    my $self = shift;
    my $parameter = $self->{'parameter'}?$self->{'parameter'}:{};
    
    my $string = "";
    foreach(keys(%$parameter)){
	$string.=" -$_ ".$parameter->{$_};
    }

    return $string;
}


=head1 ABSTRACT METHODS

=head2 B<getToolName()>

Abstract method. Has to be implemented in each sub-class

of tool. 

=cut

sub getToolName{
    my $self = shift;
    $self->{'logger'}->LogError("Abstract method 'getToolName()' not implemented.");
}


=head1 INTERNAL METHODS


=head2 B<_getExecuteStatement()>

Abstract method that has to be implemented in sub-classes.

=cut

sub _getExecuteStatement{
    my $self = shift;
    $self->{'logger'}->LogError("GePan::Tool::_createExecuteStatement() - Abstract method _CreateExecuteStatement not implemented in sub-class");
}


1;
