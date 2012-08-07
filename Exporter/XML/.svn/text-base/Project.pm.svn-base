package GePan::Exporter::XML::Project;
use base qw(GePan::Exporter::XML);

use GePan::Collection::Sequence;
use GePan::Exporter::XML::Writer::SequenceAnnotation;
use GePan::Exporter::XML::Writer::Hit;
use GePan::Exporter::XML::Writer::PredictionTool;

use strict;

use Data::Dumper;

=head1 NAME

GePan::Exporter::XML::Project

=head1 DESCRIPTION

Exports detailed information about a pipeline run in xml-format including 

Exports the annotation of all given sequences in xml-format. Just hits the annotation of a sequence was based on are shown (GePan::SequenceAnnotation object). For complete output see GePan::Exporter::XML::CompleteProject.pm

=head1 ATTRIBUTES

collection: GePan::Collection::Sequence of annotated sequences

=head1 METHODS

=head2 B<export()>

Implementation of abstract class. Exports annotation information of given sequences in XML format.

=cut

sub export{
    my $self = shift;

    $self->{'logger'}->LogError("Exporter::XML::Project::export() - No output file given.") unless $self->getFile();
    $self->{'logger'}->LogError("No sequences given for export.") unless ref $self->{'collection'};

    open(OUT,">".$self->getFile()) or $self->{'logger'}->LogError("Exporter::XML::Project::export() - Failed to open file ".$self->getFile()." for writing.");

    print OUT "<AnnotationResults>\n";
    print OUT "\t<Sequences>\n";    

    while(my $sequence = $self->{'collection'}->nextElement()){
	print OUT "\t\t<Sequence>\n";
	print OUT "\t\t\t<Sequence_ID>".$sequence->getID()."</Sequence_ID>\n";
	print OUT "\t\t\t<length>".$sequence->getLength()."</length>\n";
	print OUT "\t\t\t<start>".$sequence->getStart()."</start>\n";
	print OUT "\t\t\t<stop>".$sequence->getStop()."</stop>\n";
	print OUT "\t\t\t<sequence>".$sequence->getSequence()."</sequence>\n";

	my $sequenceAnnotation = GePan::Exporter::XML::Writer::SequenceAnnotation->new();
	$sequenceAnnotation->setParams({fh=>*OUT,
					depth=>2,
					sequence_annotation=>$sequence->getAnnotation()
					});

	print OUT "\t\t</Sequence>\n";
    }
    
    print OUT "</Sequences>\n";    
    print OUT "\t</AnnotationResults>";
    close(OUT);
}
