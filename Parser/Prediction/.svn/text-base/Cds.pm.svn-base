package GePan::Parser::Prediction::Cds;
use base qw(GePan::Parser::Prediction);
use strict;
use Data::Dumper;
use GePan::Sequence::Type::Cds;
use GePan::Sequence::Type::Contig;
use GePan::Sequence::Type::Read;


=head1 NAME

GePan::Parser::Prediction::Cds

=head1 DESCRIPTION

Sub-class of GePan::Parser;

Super-class for all parsers of prediction tools that predict CDS.

=head1 ATTRIBUTE

=head1 INTERNAL METHODS

=head2 B<_createSequences($self,contig_sequence_name,$seqObj)>

Implementation of abstract method from GePan::Parser::Prediction

Given a parent_contig name and a GePan::Sequence::Cds object (without actual sequence) it extracts the nucleotide sequence from the contig, adds it to the sequence-object and adds the sequence-object to the result collection.

NOTE: If COMPLEMENT is set, sequence start HAS TO BE BIGGER than sequence stop!

=cut

sub _createSequences{
    my ($self,$contig_name,$seqObj) = @_;

    $self->{'logger'}->LogError("No contig/read name $contig_name found in collection of parent sequences.") unless ($self->{'parent_sequences'}->getElementByID($contig_name));
    my $contig = $self->{'parent_sequences'}->getElementByID($contig_name);

    my $nuc = ($contig->getSequence());

    my $string = substr($nuc,(_getMin($seqObj->getStart(),$seqObj->getStop()))-1,$seqObj->getLength());
 
    if($seqObj->getComplement()){
        my $bioObj = Bio::Seq->new(-seq=>$string,
                                   -alphabet=>'dna');
        $seqObj->setSequence($bioObj->revcom->seq);
    }
    else{
        $seqObj->setSequence($string);
    }
    
    $self->{'logger'}->LogError("Sequence ".$seqObj->{'id'}." already exists in collection.") unless !($self->{'collection'}->getElementByID($seqObj->{'id'}));
    my $skipCount = 0 ;
    if($self->_checkInFrameStop($seqObj)){
	$self->{'logger'}->LogWarning("\nGePan::Parser::Cds::_createSequences() - In-frame stop codon found. Skipping sequence \'".$seqObj->getID()."\' :".Dumper $seqObj."\n");
	$skipCount++;
    }
    else{
	$self->{'collection'}->addElement($seqObj);
    }
}


sub _getMax{
    my ($a,$b) = @_;
    return $a unless $a<$b;
    return $b;
}

sub _getMin{
    my ($a,$b) = @_;
    return $a unless $a>$b;
    return $b;
}



sub _checkInFrameStop{
    my ($self,$seqObj) = @_;
    if($seqObj->translateSequence()=~/^[a-zA-Z]+[\*\+\.][a-zA-Z]+$/){
	return 1;
    }
    else{
	return 0;
    }
}


1;
