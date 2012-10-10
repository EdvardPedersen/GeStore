package GePan::AnnotationDBI;

use strict;
use Data::Dumper;
use DB_File;
use GePan::Annotation::Pfam;
use GePan::Annotation::Blast;
use GePan::Logger;

=head1 NAME

GePan::AnnotationDBI

=head1 DESCRIPTION

Class for reading annotation database files.

=head1 ATTRIBUTES

db_dir: directory of annotation database index and data files. 

logger: GePan::Logger object

=head1 CONSTRUCTOR

=head2 B<new()>

Returns an empty GePan::Annotation object

=cut

sub new{
    my $class = shift;
    my $self = {};
    return(bless($self,$class));
}


=head1 GETTER & SETTER METHODS

=head2 B<getAnnotation(id)>

Returns a GePan::Annotation object with given $id;

=cut

sub getAnnotation{
    my ($self,$id) = @_;
   
    $self->{'logger'}->LogError("AnnotationDBI::getAnnotation() - No database path set.") unless $self->{'db_dir'};

    my $indexFile = $self->{'db_dir'}."/index.dat";
    $self->{'logger'}->LogError("AnnotationDBI::getAnnotation() - Annotation index flat file doesn\'t exist") unless -f $indexFile;


    my $annotationFile = $self->{'db_dir'}."/annotations.dump"; 
    $self->{'logger'}->LogError("AnnotationDBI::getAnnotation() - Annotation data file doesn\'t exist.") unless -f $annotationFile;

    # connect to database and get 
    my %db;
    tie %db,  'DB_File', ($indexFile) or $self->{'logger'}->LogError("AnnotationDBI::getAnnotation() - Can't initialize database: $!\n");
    my $value = $db{$id};
    untie %db;

    ## Cutting returned string: "byte_offset||length"
    my @split = split(/\|\|/,$value);
    $self->{'logger'}->LogError("AnnotationDBI::getAnnotation() - Odd number of elements in split.") unless (scalar(@split)==2);

    my $offset = $split[0];
    my $length = $split[1];

    # open annotation file and read dataset
    open(FILE,"<$annotationFile") or $self->{'logger'}->LogError("AnnotationDBI::getAnnotation() - Failed to open annotation file $annotationFile for reading.");
    seek(FILE,$offset,0);
    my $dump;
    read(FILE,$dump,$length);
    close(FILE);

    # typecast back to ref named 'annotation' (see /db_scripts/processDat.pl for details)
    my $annotation;
    eval $dump;
    $self->{'logger'}->LogError("AnnotationDBI::getAnnotation() - Annotation not a hash ref.") unless ref $annotation;
    return $annotation; 
}

=head2 B<setDB(path)>

Sets path to the directory of annotation files annotation.dump and index.dat of this database.

=cut

sub setDB{
    my ($self,$db) = @_;
    $self->{'db_dir'} = $db;
}


=head2 B<setParams($params)>

Sets all attributes obj->{$key} = $params->{$value}

=cut

sub setParams{
    my ($self,$h) = @_;
    foreach(keys(%$h)){
	$self->{$_} = $h->{$_};
    }
}


=head2 B<setLogger(GePan::Logger)>

Sets GePan::Logger object.

=cut

sub setLogger{
    my ($self,$l) = @_;
    $self->{'logger'} = $l;
}

1; 
