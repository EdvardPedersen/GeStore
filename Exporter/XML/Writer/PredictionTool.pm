package GePan::Exporter::XML::Writer::PredictionTool;
use base qw(GePan::Exporter::XML::Writer);

use strict;
use Data::Dumper;

=head1 NAME

    GePan::Exporter::XML::Writer::PredictionTool

=head1 DESCRIPTION

Writes out prediction tool (glimmer3 or MGA) information of sequence in xml-format.

=head1 ATTRIBUTES

prediction_tool: GePan::PredictionTool object

=head1 CONSTRUCTOR

=head2 B<new()>

Creates an empty GePan::Exporter::XML::Writer::PredictionTool object.

=cut

sub new{
    my $class = shift;
    my $self = {class=>'PredictionTool'};
    return(bless($self,$class));
}

=head1 METHODS

=head2 B<export()>

Implementation of abstract method SUPER::export(). Prints given GePan::PredictionTool to self->{fh}

=cut

sub export{
    my $self = shift;
    die "[ERROR] No filehandle given for Writer::Database." unless ref $self->{'fh'};
    die "[ERROR] No GePan::PredictionTool object given for Writer::PredictionTool." unless $self->{'prediction_tool'};
    die "[ERROR] No depth set for Writer::PredictionTool object." unless  exists($self->{'depth'});

    # array of possible keys for any hit-object with scalar values
    my @params = ("score","strand","frame","contig");

    # print opening tag of class
    $self->start();
  
    $self->_writeLine("tool_name",$self->{'prediction_tool'}->getName(),($self->getDepth()+1));
 
    # print attribtues of object with scalar values 
    foreach my $key(@params){
	if(exists($self->{'prediction_tool'}->{$key})){
	    $self->_writeLine($key,$self->{'prediction_tool'}->{$key},($self->getDepth()+1));
	}
    }   
    $self->stop();
}

1;

