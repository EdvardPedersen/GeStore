package GePan::Exporter::Metarep;
use base qw(GePan::Exporter);
use GePan::Collection::Sequence;
use GePan::Mapping::Mapping;
use strict;
use Data::Dumper;
use GePan::Hit::Pfam;
use GePan::Hit::Priam;

=head1 NAME

GePan::Exporter::Metarep

=head1 DESCRIPTION

Exporter for Metarep visual representation. 

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
  
    $self -> _load();

    my $path = $self->{'output_directory'}."/".$self->{'file'};
    $path=~s/\/\//\//g;

    open(FILE,">$path") or die "Failed to open output file $path for writing.";

    my $levelSwitch = 0;

    my $seqCount = 1;

    #Create lookuptables for mapping
    my $Pfam2GO = GePan::Mapping::Mapping -> new();
    my $GO2EC = GePan::Mapping::Mapping -> new();
    $Pfam2GO -> {'logger'} = $self ->{'logger'};
    $GO2EC -> {'logger'} = $self->{'logger'};
    $Pfam2GO -> indexPfam2GO();
    $GO2EC -> indexGO2EC();

    foreach my $seq (sort{$a->getAnnotation()->getConfidenceLevel cmp $b->getAnnotation()->getConfidenceLevel()}@{$self->{'collection'}->getList()}){
	my $annotation = $seq->getAnnotation();	
	my $attributes = $annotation->getAttributeCollection();
	my $trans = $annotation->getTransferredAnnotation();
	my $func = $annotation->getFunctionalAnnotation();

	######## Scalars to print ########
	my $FunctionalDescription = $func?$func->getID()."\t":"\t";
	my $CommonNameSource = $trans?$trans->getAnnotation()->getRefSeq()."\t":"\t";
	my $GeneOntology = $func?$Pfam2GO->getPfam2GO($func->getAccessionNumber()):["\t"];
	my $SourceGeneOntology = $func?$func->getAccessionNumber()."\t":"\t";
	my $EnzymeCommission = $GeneOntology?$GO2EC->getGO2EC($GeneOntology):["\t"];
	my $EcResults = getBestEC($attributes, $EnzymeCommission);
	my $Hmm = $func?$func->getQueryName()."\t":"\t";
	my $Blast1 = $trans?$trans->getAnnotation()->getTaxonomyID()."\t":"\t";
	my $Blast2 = $trans?$trans->getEValue()."\t":"\t";
	my $Blast3 = $trans?$trans->getPercentIdentity()."\t":"\t";

	######## Values to print in each column ########
	###Peptide ID (Sequence ID)
	print FILE $seq->{'id'}."\t";
	###Library ID
	print FILE "LibraryID\t";
	###Functional Description
	print FILE $FunctionalDescription;
	###Common Name Source
	print FILE $CommonNameSource;
	###Gene Onology ID
	if ($GeneOntology->[-1] !~ m/^\t/){push (@{$GeneOntology}, "\t");}
	foreach (@$GeneOntology){
		print FILE $_;
		if($_ eq $GeneOntology->[-2]){next;}
		if($_ =~ m/^GO/){print FILE '||';}
	}
	###Source of Gene Ontology Assignment
	print FILE $SourceGeneOntology;
	###Enzyme Commission ID and Source
	print FILE $EcResults;
	###Hidden Markov Model Hits (Pfam/TIGRfam)
	print FILE $FunctionalDescription;
	###BLAST info
	print FILE $Blast1;
	print FILE $Blast2;
	print FILE $Blast3;
	print FILE "\t";
	###Filter Tag
	print FILE "Filter tag\t\n";

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

#Rewrite this. It's horrible.
sub getBestEC{
    my $attributes = $_[0];
    my $EnzymeCommission = $_[1];

    ###Prepare Priam results, if any.
    my $EC;
    my $sourceEC;
    #Get the first element, which is the lowest Evalue. Set to valid EC if >= E-10
    foreach my $priam(@{$attributes->getList()}){
	my $pec = $priam->getEC();
	my $pevalue = $priam->getEValue();
	if ($pevalue =~ /-\d{2}$/){
		$EC = $pec."\t";
		$sourceEC = "Priam; Evalue: ".$pevalue."\t";
	}
	last;
    }
    ###Prepare Pfam2GO2EC results, if any.
    sort(@$EnzymeCommission);
    #If no tab, insert tab at the end
    if ($EnzymeCommission->[0] !~ m/^\t/){push (@{$EnzymeCommission}, "\t");}
    #Extract defined values
    my @temp = grep {defined($_)} @$EnzymeCommission;
    my @resultEC;
    foreach (@temp){
	
    	push (@resultEC, $_);
    	if (($_ eq $temp[-2])||($_ eq $temp[-1])){next;}
    	push (@resultEC, '||');
    }
    ###Prioritize Priam
    if (defined($sourceEC)){
	my $return = $EC.$sourceEC;
	return $return;
    }
    ###No Priam, then Pfam2GO2EC
    elsif(defined($resultEC[1])){
	my $return = join ('',@resultEC)."Pfam2GO2EC\t";
	return $return;
    }
    ###No hits, return blank (\t\t)
    else{return "\t\t";}

}

1;
