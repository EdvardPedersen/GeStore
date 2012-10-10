package GePan::Exporter::Fasta;
use base qw(GePan::Exporter);
use GePan::Collection::Sequence;

use strict;
use Data::Dumper;

=head1 NAME

GePan::Exporter::Fasta

=head1 DESCRIPTION

Simple fasta-file writer.

=head1 ATTRIBUTES

collection: GePan::Collection::Sequence of sequences the fasta file should be written of

output_types: Types the sequencec should be: either nucleotide, protein or both seprated by ',' 

=head1 METHODS

=head2 B<export()>

Exports given sequence(s) to a fasta file of given name to given directory.

Additionally sets attribute 'tmp_file' to all sequence objects.

=cut

sub export{
    my $self = shift;

    $self->{'logger'}->LogError("GePan::Exporter::Fasta::export() - No output directory set.") unless $self->{'output_directory'};
    $self->{'logger'}->LogError("GePan::Exporter::Fasta::export() - No output file set.") unless $self->{'file'};
    $self->{'logger'}->LogError("GePan::Exporter::Fasta::export() - No sequences given.") unless $self->{'collection'};    

    my @split = split(",",$self->{'output_types'});
 
    my $input_type = $self->{'collection'}->getList()->[0]->getSequenceType();

    # check if just one output type was given that matches type of given sequences
    if((scalar(@split)==1)&&($input_type eq $split[0])){
	my $path;
	if(-d ($self->{'output_directory'}."/".$split[0])){
	    $path = $self->{'output_directory'}."/".$split[0]."/".$self->{'file'};
	}
	elsif(-d ($self->{'output_directory'})){
	    $path = $self->{'output_directory'}."/".$self->{'file'};
	}
	else{
	    $self->{'logger'}->LogError("GePan::Exporter::Fasta::export() - Export directory does not exist!");
	}
	$path=~s/\/\//\//g;
	open(FILE,">$path") or $self->{'logger'}->LogError("GePan::Exporter::Fasta::export() - Failed to open fasta file $path for writing.");
	while(my $seq = $self->{'collection'}->getNextElement()){
	    print FILE ">".$seq->{'id'}."\n".($seq->getSequence())."\n\n";
	}
	close(FILE);
    }
    elsif((scalar(@split)==2)&&($input_type eq "nucleotide")){
	$self->_exportNucleotide(); 
	$self->_exportProtein(); 
    }
    elsif((scalar(@split)==2)&&($input_type eq "protein")){
	$self->{'logger'}->LogError("GePan::Exporter::Fasta::export - can't export protein sequences to two different datatypes.");
    }
    else{
	$self->{'logger'}->LogError("GePan::Exporter::Fasta::export - wrong or mismatching input or output types.");
    }
}


=head2 B<_exportNucleotide()>

Exports given nucleotide sequences to nucleotide fastas."

=cut

sub _exportNucleotide{
    my $self = shift;
    my $path = $self->{'output_directory'}."/nucleotide/".$self->{'file'};
    $path=~s/\/\//\//g;
    open(FILE,">$path") or $self->{'logger'}->LogError("GePan::Exporter::Fasta::export() - Failed to open fasta file $path for writing.");
    while(my $seq = $self->{'collection'}->getNextElement()){
        print FILE ">".$seq->{'id'}."\n".($seq->getSequence())."\n\n";
    }
    close(FILE);
}


=head2 B<_exportProtein()>

Exports given nucleotide sequences to protein fasta.

=cut

sub _exportProtein{
    my $self = shift;
    my $path = $self->{'output_directory'}."/protein/".$self->{'file'};
    $path=~s/\/\//\//g;
    open(FILE,">$path") or $self->{'logger'}->LogError("GePan::Exporter::Fasta::export() - Failed to open fasta file $path for writing.");
    while(my $seq = $self->{'collection'}->getNextElement()){
        print FILE ">".$seq->{'id'}."\n".($seq->translateSequence())."\n\n";
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
