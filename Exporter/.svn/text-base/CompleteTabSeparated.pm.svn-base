package GePan::Exporter::CompleteTabSeparated;
use base qw(GePan::Exporter);
use GePan::Collection::Sequence;
use strict;
use Data::Dumper;

=head1 NAME

GePan::Exporter::CompleteTabSeparated

=head1 DESCRIPTION

Exporter for complete annotation. Output file is a tab-separated file of all annotations ordered by query sequence name.

=head1 ATTRIBUTES

collection: GePan::Collection::Sequence of annotated sequences

=head1 METHODS

=head2 B<export()>

Exports given sequence(s) to tab-separated file of given name to given directory.

Results are ordered by confidence level.

Additionally sets attribute 'tmp_file' to all sequence objects.

=cut

sub export{
    my $self = shift;

    die "No output directory set." unless $self->{'output_directory'};
    die "No output file name given." unless $self->{'file'};
    die "No sequence(s) given to export." unless $self->{'collection'};

    my $path = $self->{'output_directory'}."/".$self->{'file'};
    $path=~s/\/\//\//g;

    open(FILE,">$path") or die "Failed to open output file $path for writing.";

    my $levelSwitch = 0;

    my $seqCount = 1;

    foreach my $seq (sort{$a->getAnnotation()->getConfidenceLevel cmp $b->getAnnotation()->getConfidenceLevel()}@{$self->{'collection'}->getList()}){
	my $annotation = $seq->getAnnotation();
	# print Confidence level headline
	if((!$levelSwitch)||($levelSwitch ne $annotation->getConfidenceLevel())){
	    print FILE "########## Sequence(s) with confidence level ".$annotation->getConfidenceLevel()."##########\n\n";
	    $levelSwitch = $annotation->getConfidenceLevel();
	}
	
	my $trans = $annotation->getTransferredAnnotation();

	print FILE "\n#### SEQUENCE $seqCount #################";
	print FILE "\n# Name: ".$seq->{'id'};
	print FILE "\n# Start: ".$seq->getStart();
	print FILE "\n# Stop: ".$seq->getStop();
	print FILE "\n# Length: ".$seq->getLength();
	print FILE "\n# Annotation Confidence: $levelSwitch";
	print FILE "\n#";
	print FILE "\n### ANNOTATION DETAILS ##########";
	print FILE "\n## TRANSFERRED ########";
	print FILE "\n# Hit name: ".$trans->getID();
	print FILE "\n# Transferred confidence level: ".$trans->getAnnotation()->getConfidenceLevel();
	print FILE "\n# Tool name: ".$trans->{'tool'};
	print FILE "\n# Database: ".$trans->{'database'}->{'name'};
	print FILE "\n# ANNOTATION: ".$trans->getAnnotation()->{'annotation'} unless !($trans->{'annotation'});
	print FILE "\n# Hit taxon ID: ".$trans->getAnnotation()->{'taxonomy_id'} unless !($trans->getAnnotation()->{'taxonomy_id'});
	print FILE "\n# Organism: ".$trans->getAnnotation()->getOrganism() unless !($trans->getAnnotation());
	print FILE "\n# Embl accession Nr.: ".$trans->getAnnotation()->{'embl'} unless !($trans->{'embl'});
	print FILE "\n# Score\tE-Value\t%_identity\t%_similarity";
	print FILE "\n".$trans->getScore()."\t".$trans->getEValue()."\t".$trans->{'percent_identity'}."\t".$trans->{'percent_similarity'};

	my $func = $annotation->getFunctionalAnnotation();

	print FILE "\n#";
	print FILE "\n## FUNCTIONAL ########";
	print FILE "\n# Hit name: ".$func->getID();
	print FILE "\n## Functional confidence level: ".$func->getAnnotation()->getConfidenceLevel();
	print FILE "\n# Database: ".$func->{'database'}->{'name'};
	print FILE "\n# Type: ".$func->getAnnotation()->getType();
	print FILE "\n# DESCRIPTION: ".$func->getAnnotation()->{'description'} unless !($func->{'description'});
	print FILE "\n# Score\tE-Value\tdomain-length\taccuracy";
	print FILE "\n".$func->getScore()."\t".$func->getEValue()."\t".$func->getLength()."\t".$func->getAccuracy();
	print FILE "\n###########################################";
	print FILE "\n";
	$seqCount++;
    }
    close(FILE);
}



=head1 GETTER & SETTER METHODS

=head2 B<setCollection(GePan::Collection::Sequence)>

Sets the sequence-object(s) of the exporter.

=cut

sub setCollection{
    my ($self,$seqs) = @_;
    $self->{'collection'} = $seqs;
}

=head2 B<getCollection()>

Returns the GePan::Collection::Sequence object.

=cut

sub getCollection{
    my $self = shift;
    return $self->{'collection'};
}

1;
