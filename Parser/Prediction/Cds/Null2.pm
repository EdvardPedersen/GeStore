package GePan::Parser::Prediction::Cds::Null2;
use base qw(GePan::Parser);
use GePan::Logger;

=head1 NAME

GePan::Parser::Input::Fasta

=head1 DESCRIPTION

Basic parser for fasta files (multiple as well as single sequences).

Creates objects of type GePan::Sequence

=cut

=head1 ATTRIBUTES

type = any sequence type supported by GePan, e.g. contig, read, cds or protein

collection = GePan::Collection::Sequence object

order = array-ref of sequence names in the order they appear in the parsed file

=cut

use strict;
use warnings;
use Data::Dumper;
use GePan::Sequence::Type::Contig;
use GePan::Sequence::Type::Read;
use GePan::Sequence::Type::Protein;
use GePan::Sequence::Type::Cds;
use GePan::Logger;

=head1 METHODS

=head2 B<parseFile(file)>

Reads in the fasta file specified by file.

=cut

sub parseFile{
    my $self = shift;

    my $file = $self->{'file'};

    my $collection = GePan::Collection::Sequence->new();
    my $name = "";
    my $seq = "";
    my $order = [];

#    $self->{'logger'}->LogError("No fasta file type given") unless $self->{'type'};
    $self->{'logger'}->LogError("No fasta file given") unless $self->{'file'};
    $self->{'logger'}->LogError("Path to given fasta file ".$self->{'file'}."does not exist") unless (-f ($self->{'file'}));

    open(FILE,"<$file") or $self->{'logger'}->LogError("Failed to open fasta file $file for reading.");
    while(<FILE>){
        if($_=~/^>(.*)[\r\s\t\n]*$/){
            if($name){
                $self->{'logger'}->LogError("Gene ".$name." already in gene-list!") if ($collection->getElementByID($name));
		my $h = {id=>$name,
                         start=>'1',
                         stop=>length($seq),
                         sequence=>$seq,
                         };
		my $class = "GePan::Sequence::Type::Read";
		eval{ _createSequence($h,$class,$collection);};
		if($@){$self->{'logger'}->LogError($@);}
                $name = "";
                $seq="";
            }
            my $tmp = $1;
	    my @nameSplit = split(" ",$tmp);
	    $name = $nameSplit[0];
	    $name=~s/[\s\r\t\n]//g;
	    if($name=~/^([a-zA-Z\d\-_]+)[\s\t]+[\w]+.*$/){
		$name = $1;
	    }
	    push @$order,$name;
        }
        else{
            if($_=~/^([A-Za-z]+)[\n\t\s\*]*$/){
                $seq.=$1;
            }
        }
    }
    close(FILE);

    # insert the last sequence
    my $class = "GePan::Sequence::Type::Read";
    my $h = { id=>$name,
	start=>1,
	stop=>length($seq),
	sequence=>$seq,
	logger=>$self->{'logger'}};
    eval{_createSequence($h,$class,$collection);};
    if($@){ $self->{'logger'}->LogError($@);}
    $self->{'collection'} = $collection;
    $self->{'order'} = $order;
}


=head1 GETTER & SETTER METHODS


=head2 B<getOrder()>

Returns array ref of sequence-names in the same order they are parsed from file

=cut

sub getOrder{
    my $self = shift;
    return $self->{'order'};
}


=head2 B<setType(type)>

Sets fast file type to given type.

=cut

sub setType{
    my ($self,$t) = @_;
    $self->{'logger'}->LogError("Unkown fasta type given.") unless (($t eq 'read')||($t eq 'contig')||($t eq 'nucleotide')||($t eq 'protein'));
    $self->{'type'} = $t;
}

=head2 B<getType()>

Returns type of fasta file.

=cut

sub getType{
    my $self = shift;
    return $self->{'type'};
}


=head2 B<getCollection()>

Returns GePan::Collection::Sequence object

=cut
sub getCollection{
    my $self = shift;
    if($self->{'collection'}){
	return $self->{'collection'};
    }
    else{
	return 0;
    }
}


=head2 B<getNumberOfSequences()>

Returns the number of sequences in this sequence object.
Returns -1 if self->{'collection'} if undef.

=cut

sub getNumberOfSequences{
    my $self = shift;
    return scalar(@{$self->{'collection'}->getList()});
}


=head1 INTERNAL METHODS

=head2 B<_createSequence()>

Creates a GePan::Sequence object of appropriate type and adds it to collection

=cut

sub _createSequence{
    my ($h,$class,$collection) = @_;
    my $seqObj = $class->new();
    $seqObj->setParams($h);
    $collection->addElement($seqObj);
}   


1;
