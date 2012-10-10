package GePan::Exporter::XML::Writer::Hit;
use base qw(GePan::Exporter::XML::Writer);

use strict;
use Data::Dumper;
use GePan::Exporter::XML::Writer::Database;
use GePan::Exporter::XML::Writer::Annotation;
=head1 NAME

    GePan::Exporter::XML::Writer::Hit

=head1 DESCRIPTION

Writes a GePan::Hit::XXX object in xml-format to given filehandle.

=head1 ATTRIBUTES

hit: GePan::SequenceAnnotation object that's to be printed.

=head1 CONSTRUCTOR

=head2 B<new()>

Creates an empty GePan::Exporter::XML::Writer::SequenceAnnotation object.

=cut

sub new{
    my $class = shift;
    my $self = {class=>'Hit'};
    return(bless($self,$class));
}

=head1 METHODS

=head2 B<export()>

Implementation of abstract method SUPER::export(). Prints given GePan::SequenceAnnotation to self->{fh}

=cut

sub export{
    my $self = shift;

    $self->{'logger'}->LogError("GePan::Exporter::XML::Writer::Hit - No filehandle given for Writer::Hit.") unless ref $self->{'fh'};
    $self->{'logger'}->LogError("GePan::Exporter::XML::Writer::Hit - No depth set for Writer::Hit object.") unless  defined($self->{'depth'});

    # print opening tag of class
    $self->start();

    if(ref ($self->{'hit'})){
	# get all values that have to be exported
	my $params = $self->{'hit'}->_getAttributes();
	# print attribtues of object with scalar values 
	foreach my $key(@$params){
	    if(exists($self->{'hit'}->{$key})){
		$self->_writeLine($key,$self->{'hit'}->{$key},($self->getDepth()+1));
	    }
	}   

	if($self->{'hit'}->{'annotation'}){
	    # print Annotation of hit
	    my $annotationWriter = GePan::Exporter::XML::Writer::Annotation->new();
	    $annotationWriter->setParams({annotation=>$self->{'hit'}->getAnnotation(),
				      depth=>($self->{'depth'}+1),
				      fh=>$self->{'fh'}
				      });
	    $annotationWriter->export();
	}

	if($self->{'hit'}->{'database'}){
	    # print database of hit
	    if(($self->{'hit'})&&($self->{'hit'}->{'database'})){
		my $dbWriter = GePan::Exporter::XML::Writer::Database->new();
		$dbWriter->setParams({database=>$self->{'hit'}->{'database'},
				  depth=>($self->{'depth'}+1),
				  fh=>$self->{'fh'}
				 });
		$dbWriter->export();
	    }
	}
    }
    $self->stop();
}

1;

