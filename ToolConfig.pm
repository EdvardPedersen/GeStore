package GePan::ToolConfig;

use strict;
use Data::Dumper;


=head1 NAME

GePan::ToolConfig

=head1 DESCRIPTION

Class to hold all configurations for each tool that is available for this GePan installation.

=head1 ATTRIBUTES

id: Name of the tool

type: type of tools, ether prediction or annotation

input_type: either nucleotide or protein

input_sequence_type: sequence type the tool can process, e.g. cds or contig

output_type: output type of tool, if both types (protein and nucleotide) are supported both are split by ','

output_sequence_type: For example cds or rna

output_format: format of the output file (if any)

parser: name of the parser class of tool


=head1 CONSTRUCTOR

=head1 B<new()>

Returns an empty GePan::ToolConfig  object.

=cut

sub new{
    my $class = shift;
    my $self = {};
    return(bless($self,$class));
}


=head1 GETTER & SETTER METHODS


=head2 B<setType()>

Sets type of the tool, e.g. prediction.

=cut

sub setType(){
    my  ($self,$t) = @_;
    $self->{'type'} = $t;
}


=head2 B<getType()>

Returns type of tool, e.g. annotation.

=cut

sub getType{
    my $self = shift;
    return $self->{'type'};
}



=head2 B<setID(string)>

Sets the id (name) of the tool

=cut

sub setID{
    my ($self,$name) = @_;
    $self->{'id'} = $name;
}


=head2 B<getID()>

Returns the id (name) of the tool.

=cut

sub getID{
    my $self = shift;
    return $self->{'id'};
}   


=head2 B<setInputType(string)>

Sets the input type this tool can be run on.

=cut

sub setInputType{
    my ($self,$string) = @_;
    $self->{'input_type'} = $string;
}


=head2 B<getInputType()>

Returns the input type this tool can be run on.

=cut

sub getInputType{
    my $self = shift;
    return $self->{'input_type'};
}



=head2 B<setInputSequenceType(string)>

Sets the sequence type this tool can be run on, e.g. cds or contig.

=cut

sub setInputSequenceType{
    my ($self,$string) = @_;
    $self->{'input_sequence_type'} = $string;
}



=head2 B<setInputFormat(format)>

Sets format of input files.

=cut

sub setInputFormat{
    my ($self,$format) = @_;
    $self->{'input_format'} = $format;
}


=head2 B<getInputFormat()>

Returns format of input file for this tool.

=cut

sub getInputFormat{
    my $self = shift;
    return $self->{'input_format'};
}


=head2 B<setOutputFormat(format)>

Sets format of output files.

=cut

sub setOutputFormat{
    my ($self,$format) = @_;
    $self->{'output_format'} = $format;
}


=head2 B<getOutputFormat()>

Returns format of output file for this tool.

=cut

sub getOutputFormat{
    my $self = shift;
    return $self->{'output_format'};
}

=head2 B<setGsOutputFormat(format)>

Sets format of output files.

=cut

sub setGsOutputFormat{
    my ($self,$format) = @_;
    $self->{'gs_output_format'} = $format;
}


=head2 B<getGsOutputFormat()>

Returns format of output file for this tool.

=cut

sub getGsOutputFormat{
    my $self = shift;
    return $self->{'gs_output_format'};
}

=head2 B<setGsOutputFormat(format)>

Sets format of output files.

=cut

sub setGsInputFormat{
    my ($self,$format) = @_;
    $self->{'gs_input_format'} = $format;
}


=head2 B<getGsOutputFormat()>

Returns format of output file for this tool.

=cut

sub getGsInputFormat{
    my $self = shift;
    return $self->{'gs_input_format'};
}

=head2 B<setGsExporterOutputFormat(format)>

Sets format of output files.

=cut

sub setGsExporterOutputFormat{
    my ($self,$format) = @_;
    $self->{'gs_exporter_output_format'} = $format;
}


=head2 B<getGsExporterOutputFormat()>

Returns format of output file for this tool.

=cut

sub getGsExporterOutputFormat{
    my $self = shift;
    return $self->{'gs_exporter_output_format'};
}

=head2 B<getInputSequenceType()>

Returns the input type this tool can be run on.

=cut

sub getInputSequenceType{
    my $self = shift;
    return $self->{'input_sequence_type'};
}


=head2 B<setOutputType(string)>

Sets the output type this tool can be run on, e.g. nucleotide or protein.

=cut

sub setOutputType{
    my ($self,$string) = @_;
    $self->{'output_type'} = $string;
}


=head2 B<getOutputType()>

Returns the output type this tool can be run on.

=cut

sub getOutputType{
    my $self = shift;
    return $self->{'output_type'};
}

=head2 B<setOutputSequenceType(string)>

Sets the sequence type this tool can be run on, e.g. cds or contig.

=cut

sub setOutputSequenceType{
    my ($self,$string) = @_;
    $self->{'output_sequence_type'} = $string;
}


=head2 B<getOutputSequenceType()>

Returns the output type this tool can be run on.

=cut

sub getOutputSequenceType{
    my $self = shift;
    return $self->{'output_sequence_type'};
}


=head2 B<setParser(string)>

Sets name of the parser class the result files are parsed with.

=cut

sub setParser{
    my ($self,$parser) = @_;
    $self->{'parser'} = $parser;
}


=head2 B<getParser()>

Returns name of parser class for tool result files.

=cut

sub getParser{
    my $self = shift;
    return $self->{'parser'};
}


=head2 B<setParams(hash-ref)>

Takes hash-ref of format {ATTRIBUTE_NAME => ATTRIBUTE_VALUE} and set all attributes of this tool to given values.

=cut

sub setParams{
    my ($self,$h) = @_;
    foreach my $a (keys(%$h)){
	$self->{$a} = $h->{$a};
    }
}
 

1;
