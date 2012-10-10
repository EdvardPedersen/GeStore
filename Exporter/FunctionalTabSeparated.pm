package GePan::Exporter::FunctionalTabSeparated;
use base qw(GePan::Exporter);
use GePan::Collection::Sequence;
use strict;
use Data::Dumper;

=head1 NAME

GePan::Exporter::FunctionalTabSeparated

=head1 DESCRIPTION

Exporter for functional annotation. Output file is a tab-separated file of all annotations ordered by functional class.

=head1 ATTRIBUTES

collection: GePan::Collection::Sequence of annotated sequences

=head1 METHODS

=head2 B<export()>

Exports given sequence(s) to a fasta file of given name to given directory.

Additionally sets attribute 'tmp_file' to all sequence objects.

=cut

sub export{
    my $self = shift;

    die "No output directory set." unless $self->{'output_directory'};
    die "No output file name given." unless $self->{'file'};
    die "No sequence(s) given to export." unless $self->{'collection'};

    my $path = $self->{'output_directory'}."/".$self->{'file'};
    $path=~s/\/\//\//g;

    my $sorted = _sort($self);

    open(FILE,">$path") or die "Failed to open output file $path for writing.";
    foreach my $key (keys(%$sorted)){
	my $tmpHit = $sorted->{$key}->[0]->getAnnotation()->getFunctionalAnnotation();
	my $tmpAnnotation = $tmpHit->getAnnotation();

	my $id = $tmpAnnotation->getID()?$tmpAnnotation->getID():" - ";
	warn "[WARNING] No annotation ID given!" unless $id ne " - ";;
	print FILE "## ".$id."\n";

	my $type = $tmpAnnotation->getType()?$tmpAnnotation->getType():" - ";
	warn "[WARNING] No annotation type given!" unless $type ne " - ";
	print FILE "# Type: ".$type."\n";

	my $length = $tmpHit->getLength()?$tmpHit->getLength():" - ";
	warn "[WARNING] No annotation length given!" unless $length ne " - ";
	print FILE "# Length of domain: ".$length."\n";

	my $accession = $tmpAnnotation->getAccession()?$tmpAnnotation->getAccession():" - ";
	warn "[WARNING] No annotation accession number given!" unless $accession ne " - ";
	print FILE "# Accession number: ".$accession."\n";

	my $desc = $tmpAnnotation->getDescription()?$tmpAnnotation->getDescription():" - ";
	warn "[WARNING] No annotation description given!" unless $desc ne " - ";
	print FILE "# Description: ".$desc."\n";

	print FILE "# Gene name\tconfidence level\te-value\tscore\thit-significance\n";
	my $sequences = $sorted->{$key};
	foreach(sort{$a->getAnnotation()->getFunctionalAnnotation()->getAnnotation()->getConfidenceLevel()<=>$b->getAnnotation()->getFunctionalAnnotation()->getAnnotation()->getConfidenceLevel()}@$sequences){
	    die "[ERROR] No significance for hit given." unless $_->getAnnotation->getFunctionalAnnotation();
	    my $hit = $_->getAnnotation->getFunctionalAnnotation();
	    my $sig = $hit->getSignificance()?"1":"0";
	    print FILE $_->getID()."\t".$hit->getAnnotation->getConfidenceLevel()."\t".$hit->getEValue()."\t".$hit->getScore()."\t$sig\n"
	}
	print FILE "\n";
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

=head1 INTERNAL METHODS

=head2 <_sort()>

Sorts sequences by functional domain theyve been annotated with.

Return hash-ref of form { annotation_id => sequence-obj }

=cut

sub _sort{
    my $self = shift;
    my $collection = $self->{'collection'};
    my $result = {};

    foreach my $sequence (@{$collection->getList()}){
	next unless $sequence->getAnnotation->getFunctionalAnnotation();
	if(($result->{$sequence->getAnnotation()->getFunctionalAnnotation()->getAnnotation()->getID()})&&(ref($result->{$sequence->getAnnotation()->getFunctionalAnnotation()->getAnnotation()->getID()}))){
	    push @{$result->{$sequence->getAnnotation()->getFunctionalAnnotation()->getAnnotation()->getID()}}, $sequence;
	}
	else{
	    $result->{$sequence->getAnnotation()->getFunctionalAnnotation()->getAnnotation()->getID()} = [$sequence];
	}
    }
    return $result;
}

1;
