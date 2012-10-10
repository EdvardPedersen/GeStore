package GePan::Exporter::XML::Writer::Database;
use base qw(GePan::Exporter::XML::Writer);

use strict;
use Data::Dumper;

=head1 NAME

    GePan::Exporter::XML::Writer::Database

=head1 DESCRIPTION

Writes a database-hash from GePan::Database object in xml-format to given filehandle.

=head1 ATTRIBUTES

database: hash-ref that's to be printed.

=head1 CONSTRUCTOR

=head2 B<new()>

Creates an empty GePan::Exporter::XML::Writer::SequenceAnnotation object.

=cut

sub new{
    my $class = shift;
    my $self = {class=>'Database'};
    return(bless($self,$class));
}

=head1 METHODS

=head2 B<export()>

Implementation of abstract method SUPER::export(). Prints given GePan::SequenceAnnotation to self->{fh}

=cut

sub export{
    my $self = shift;
    die "[ERROR] No filehandle given for Writer::Database." unless ref $self->{'fh'};
    die "[ERROR] No GePan::Database object given for Writer::Database." unless $self->{'database'};
    die "[ERROR] No depth set for Writer::Database object." unless  exists($self->{'depth'});

    # array of possible keys for any hit-object with scalar values
    my @params = ("format","name","type","path","taxon");

    # print opening tag of class
    $self->start();
   
    # print attribtues of object with scalar values 
    foreach my $key(@params){
	if(exists($self->{'database'}->{$key})){
	    $self->_writeLine($key,$self->{'database'}->{$key},($self->getDepth()+1));
	}
    }   
    $self->stop();
}

1;

