package GePan::Exporter::XML::QueryHits;
use base qw(GePan::Exporter::XML);

use GePan::Collection::Hit;
use GePan::Exporter::XML::Writer::Hit;

use strict;

use Data::Dumper;

=head1 NAME

GePan::Exporter::XML::QueryHits

=head1 DESCRIPTION

Exports information about all hits of one particular query sequence.

=head1 ATTRIBUTES

collection: GePan::Collection::Hit of all hits of one query sequence. 

output_directory

=head1 METHODS

=head2 B<export()>

Implementation of abstract class. Exports given hit data in XML format.

=cut

sub export{
    my $self = shift;

    $self->{'logger'}->LogError("Exporter::XML::QueryHits::export() - No output file given.") unless $self->getFile();
    $self->{'logger'}->LogError("Exporter::XML::QueryHits::export() - No output directory given") unless $self->{'output_directory'};
    $self->{'logger'}->LogError("Exporter::XML::QueryHits::export() - No sequences given for export.") unless ref $self->{'collection'};

    my $file_name = $self->{'output_directory'}."/".$self->{'file'};
    $self->{'logger'}->LogError("Exporter::XML::QueryHits::export() - Output file $file_name already exists.") if (-e $file_name);

    open(OUT,">$file_name") or $self->{'logger'}->LogError("Exporter::XML::QueryHits::export() - Failed to open file ".$self->getFile()." for writing.");

    while(my $hit = $self->{'collection'}->getNextElement()){
	my $writer = GePan::Exporter::XML::Writer::Hit->new();
	$writer->setParams({fh=>\*OUT,
			    depth=>'-1',
			    hit=>$hit
			    });
	$writer->export();
    }
    close(OUT);
}


=head2 B<_open()>

Opens the xml output file for hits to export

=cut

sub _open{
    my  $self = shift;

    $self->{'logger'}->LogError("Exporter::XML::QueryHits::_open() - No output file given.") unless $self->getFile();
    $self->{'logger'}->LogError("Exporter::XML::QueryHits::_open() - No output directory given") unless $self->{'output_directory'};

    my $file_name = $self->{'output_directory'}."/".$self->{'file'};
    if(-e $file_name){
	open(OUT,">>$file_name") or $self->{'logger'}->LogError("Exporter::XML::QueryHits::_open() - Failed to open file ".$self->getFile()." for writing.");
	$self->{'fh'} = *OUT;
    } 
    else{
	open(OUT,">$file_name") or $self->{'logger'}->LogError("Exporter::XML::QueryHits::_open() - Failed to open file ".$self->getFile()." for writing.");
	$self->{'fh'} = *OUT;
    }
}


=head2 B<addHit()>

Exports data of another hit in XML format to file. (Method \'open\' has to be called first);

=cut

sub addHit{
    my ($self,$hit) = @_;

    $self->{'logger'}->LogError("Exporter::XML::QueryHits::addHit() - No opened filehandle found.") unless ($self->{'fh'});

    my $writer = GePan::Exporter::XML::Writer::Hit->new();
    $writer->setParams({fh=>\*OUT,
                            depth=>'0',
                            hit=>$hit
                            });
    $writer->export();
}


=head2 B<_close>

Closes XML result file.

=cut

sub _close{
    my $self = shift;
    
    $self->{'logger'}->LogError("Exporter::XML::QueryHits::_close() - No opened filehandle given" unless $self->{'fh'};
    my $fh = $self->{'fh'};
    close($fh);
}


1;
