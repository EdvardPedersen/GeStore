package GePan::Exporter::XML::Writer::Annotation;
use base qw(GePan::Exporter::XML::Writer);

use strict;
use Data::Dumper;

=head1 NAME

    GePan::Exporter::XML::Writer::Annotation

=head1 DESCRIPTION

Writes a GePan::Annotation::XXX object in xml-format to given filehandle.

=head1 ATTRIBUTES

annotation: GePan::Annotation object that's to be printed.

=head1 CONSTRUCTOR

=head2 B<new()>

Creates an empty GePan::Exporter::XML::Writer::Annotation object.

=cut

sub new{
    my $class = shift;
    my $self = {class=>'Annotation'};
    return(bless($self,$class));
}

=head1 METHODS

=head2 B<export()>

Implementation of abstract method SUPER::export(). Prints given GePan::SequenceAnnotation to self->{fh}

=cut

sub export{
    my $self = shift;
    $self->{'logger'}->LogError("Exporter::XML::Writer::Annotation::export() - No filehandle given for Writer::Annotation.") unless ref $self->{'fh'};
    $self->{'logger'}->LogError("Exporter::XML::Writer::Annotation::export() - No GePan::Hit object given for Writer::Annotation.") unless $self->{'annotation'};
    $self->{'logger'}->LogError("Exporter::XML::Writer::Annotation::export() - No depth set for Writer::Annotation object.") unless  exists($self->{'depth'});

    # array of possible keys for any hit-object with scalar values
    my @params = ("pfam","ref_seq","embl","confidence","id","description","pir","taxonomy_id","accession","type","gathered_threshold","organism");

    # print opening tag of class
    $self->start();
   
    # print attribtues of object with scalar values 
    foreach my $key(@params){
	if(exists($self->{'annotation'}->{$key})){
	    $self->_writeLine($key,$self->{'annotation'}->{$key},($self->getDepth()+1));
	}
    }   
    $self->stop();
}

1;

