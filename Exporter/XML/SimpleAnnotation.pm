package GePan::Exporter::XML::SimpleAnnotation;
use base qw(GePan::Exporter::XML);

use GePan::Collection::Sequence;
use GePan::Exporter::XML::Writer::Hit;
use GePan::Exporter::XML::Writer::PredictionTool;
use GePan::Exporter::XML::Writer::SequenceAnnotation;
use GePan::Config qw(GEPAN_PATH);

use strict;

use Data::Dumper;

=head1 NAME

GePan::Exporter::XML::SimpleAnnotation

=head1 DESCRIPTION

Exports the annotation of all given sequences in xml-format. Just hits the annotation of a sequence was based on are shown (GePan::SequenceAnnotation object). For complete output see GePan::Exporter::XML::CompleteProject.pm

=head1 ATTRIBUTES

collection: GePan::Collection::Sequence of annotated sequences

=head1 METHODS

=head2 B<export()>

Implementation of abstract class. Exports annotation information of given sequences in XML format.

=cut

sub export{
    my $self = shift;

    $self->{'logger'}->LogError("GePan::Exporter::XML::SimpleAnnotation::export() - No name of output file given.") unless $self->getFile();
    $self->{'logger'}->LogError("GePan::Exporter::XML::SimpleAnnotation::export() - No path to output directory given.") unless $self->{'output_directory'};
    $self->{'logger'}->LogError("GePan::Exporter::XML::SimpleAnnotation::export() - No sequences given for export.") unless ref $self->{'collection'};

    my $output_file = $self->getOutputDir()."/".$self->getFile();
    $output_file=~s/\/\//\//g;



    open(OUT,">$output_file") or $self->{'logger'}->LogError("GePan::Exporter::XML::SimpleAnnotation::export() - Failed to open file $output_file for writing.");

    print OUT "<AnnotationResults>\n";
    print OUT "\t<Sequences>\n";    

    while(my $sequence = $self->{'collection'}->getNextElement()){
	print OUT "\t\t<Sequence>\n";
	print OUT "\t\t\t<Sequence_ID>".$sequence->getID()."</Sequence_ID>\n";
	print OUT "\t\t\t<length>".$sequence->getLength()."</length>\n";
	print OUT "\t\t\t<start>".$sequence->getStart()."</start>\n";
	print OUT "\t\t\t<stop>".$sequence->getStop()."</stop>\n";
	print OUT "\t\t\t<sequence>".$sequence->getSequence()."</sequence>\n";
	if($sequence->{'codon_table'}){
	    print OUT "\t\t\t<codon_table>".$sequence->{'codon_table'}."</codon_table>\n";
	}

	my $fh = \*OUT;

	# If prediction was performed print prediction tool information
	if($sequence->{'prediction_tool'}){
	    _loadPredictionTools();
	    my $predictionTool = GePan::Exporter::XML::Writer::PredictionTool->new();
	    $predictionTool->setParams({fh=>$fh,
					prediction_tool=>$sequence->{'prediction_tool'},
					depth=>2,
					logger=>$self->{'logger'}
					});
	    $predictionTool->export();
	}

	my $sequenceAnnotation = GePan::Exporter::XML::Writer::SequenceAnnotation->new();


	$sequenceAnnotation->setParams({fh=>$fh,
					depth=>2,
					sequence_annotation=>$sequence->getAnnotation(),
					logger=>$self->{'logger'}
					});
	$sequenceAnnotation->export();
	print OUT "\t\t</Sequence>\n";
    }
    
    print OUT "\t</Sequences>\n";    
    print OUT "</AnnotationResults>";
    close(OUT);
}


sub _loadPredictionTools{

    my $collectionDir = GEPAN_PATH."/GePan/PredictionTool";
    opendir(DIR,$collectionDir);
    my @classes = grep{$_=~/.*\.pm/}readdir(DIR);
    closedir(DIR);
    foreach(@classes){
        my $class = $collectionDir."/$_";
        eval{_requireClass($class)};
        die $@ if $@;
    }
}


=head2 B<_requireClass(string)>

Loads class of name string.
=cut

sub _requireClass{
    my $class = shift;
    require $class;
}



1;
