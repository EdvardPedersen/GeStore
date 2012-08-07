package GePan::Exporter::XML::Writer::SequenceAnnotation;
use base qw(GePan::Exporter::XML::Writer);

use strict;
use Data::Dumper;
use GePan::SequenceAnnotation;

=head1 NAME

    GePan::Exporter::XML::Writer::SequenceAnnotation

=head1 DESCRIPTION

Writes a GePan::SequenceAnnotation object in xml-format to given filehandle.

=head1 ATTRIBUTES

sequence_annotation: GePan::SequenceAnnotation object that's to be printed.

=head1 CONSTRUCTOR

=head2 B<new()>

Creates an empty GePan::Exporter::XML::Writer::SequenceAnnotation object.

=cut

sub new{
    my $class = shift;
    my $self = {class=>'SequenceAnnotation'};
    return(bless($self,$class));
}

=head1 METHODS

=head2 B<export()>

Implementation of abstract method SUPER::export(). Prints given GePan::SequenceAnnotation to self->{fh}

=cut

sub export{
    my $self = shift;
    $self->{'logger'}->LogError("GePan::Exporter::XML::Writer::SequenceAnnotation::export() - No filehandle given for Writer::SequenceAnnotation.") unless(ref($self->{'fh'}));
    $self->{'logger'}->LogError("GePan::Exporter::XML::Writer::SequenceAnnotation::export() - No GePan::SequenceAnnotation object given for Writer::SequenceAnnotation.") unless $self->{'sequence_annotation'};
    $self->{'logger'}->LogError("GePan::Exporter::XML::Writer::SequenceAnnotation::export() - No depth set for Writer::SequenceAnnotation object.") unless  (exists($self->{'depth'}));

    # print opening tag of class
    $self->start();
    
    # write confidence level
    $self->_writeLine("confidenceLevel",$self->{'sequence_annotation'}->getConfidenceLevel(),($self->getDepth()+1));
   
    # write transferred hit 
    my $transferred = GePan::Exporter::XML::Writer::Hit->new();
    my $trans = $self->{'sequence_annotation'}->getTransferredAnnotation()?$self->{'sequence_annotation'}->getTransferredAnnotation():0;
    $transferred->setParams({depth=>($self->getDepth()+1),
			     fh=>$self->getFH(),
			     class=>'transferred',
			     hit=>$trans,
			     logger=>$self->{'logger'}
			    });
    $transferred->export();

    # write functional hit
    my $functional = GePan::Exporter::XML::Writer::Hit->new();
    $functional->setParams({depth=>($self->getDepth()+1),
			    fh=>$self->{'fh'},
			    class=>'functional',
			    logger=>$self->{'logger'},
			    hit=>$self->{'sequence_annotation'}->getFunctionalAnnotation()
			    });
    $functional->export();

    # write attribute hits
    my $atts = $self->{'sequence_annotation'}->getAttributeCollection();
    if($atts->getSize()){
	while(my $aHit = $atts->getNextElement()){
	    my $aWriter = GePan::Exporter::XML::Writer::Hit->new();
	    $aWriter->setParams({depth=>($self->getDepth()+1),
				 fh=>$self->{'fh'},
				 class=>$aHit->getToolName(),
				 logger=>$self->{'logger'},
				 hit=>$aHit});
	    $aWriter->export();
	}
    }    


    $self->stop();
}

1;

