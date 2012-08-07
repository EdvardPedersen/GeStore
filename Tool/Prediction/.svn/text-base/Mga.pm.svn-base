package GePan::Tool::Prediction::Mga;
use base qw(GePan::Tool);

use strict;
use GePan::Config qw(MGA_PATH);
use GePan::Logger;

=head1 NAME

GePan::Tool::Prediction::Mga

=head1 DESCRIPTION

Class for executing MetaGeneAnnotator gene prediction tool.

Sub-class of GePan::Tool

=head1 ATTRIBUTES

species = if sequences should be treated as derived from one species (s) or multiple species (m)

=head1 METHODS

=head2 B<execute()>

Executes one MetaGeneAnnotator with given input file.

Output is written to given output dir.

=cut

sub execute{
    my $self = shift;

    my $statement = $self->_getExecuteStatement();

    $self->{'logger'}->LogStatus("Mga execute statement: $statement"); 
    my $out = `$statement`;
}

=head1 GETTER & SETTER METHODS

=head2 <setSpecies(species)>

Sets species to either single (s) or multiple (m)

=cut

sub setSpecies{
    my ($self,$species) = @_;
    if(!(($species eq 's')||($species eq 'm'))){
	$self->{'logger'}->LogError("Given species type is neither \'s\' nor \'m\'.");
    }
    $self->{'species'} = $species;
}

=head2 B<getSpecies()>

Returns type of species (either s = single or m = multiple);

=cut

sub getSpecies{
    my $self = shift;
    return $self->{'species'};
}


=head2 B<getToolName()>

Returns short name of tool (\'mga\')

=cut

sub getToolName{
    my $self = shift;
    return "mga";
}

=head1 INTERNAL METHODS

=head2 B<_createOutputName()>

Creates name of output file.

output_file = mga_prediction.out.JOBID

=cut

sub _createOutputName{
    my $self = shift;
    my $name = "mga_prediction.out.".$self->{'job_id'};
    $self->{'output_file'} = $name;
}


=head2 B<_getExecuteStatement()>

Creates and returns the execute statement for glimmer.

=cut

sub _getExecuteStatement{
    my $self = shift;

    if(!$self->{'output_file'}){
        $self->{'output_file'} = _createOutputName($self);
    }

    my $outputPath = $self->{'output_dir'}.'/'.$self->{'output_file'};

    my $statement = MGA_PATH." ".$self->{'input_file'};
    $statement.= "-m " unless $self->{'species'} ne 'm';
    $statement.= " > $outputPath";
    
    return $statement;
}



1;
