package GePan::Databases;

use strict;
use Data::Dumper;
use GePan::Config qw(DATABASE_PATH);

=head1 NAME

GePan::Databases

=head1 DESCRIPTION

Package for providing information about available sequence databases.

All databases available on the system have to be defined here (in method _getDBs)!

=head1 ATTRIBUTES

databases : hash-ref of 
    
    {database_name=>{ 
    
	name=> DB_NAME, 

	path=>DB_PATH, 

	type => DB_TYPE, 

	format => DB_FORMAT, 

	taxon=>DATABASE_TAXON}

    }

For details on database attributes see getDBs()

=head1 CONSTRUCTOR

=head2 B<new()>

Returns new GePan::Databases object;

=cut

sub new{
    my $class = shift;
    my $self = {'index'=>0,
		databases=>_getDBs()};
    return (bless($self,$class));
}

=head1 METHODS

=head2 B<getDatabases()>

Returns list of databases

=cut

sub getDatabases{
    my $self  = shift;
    return $self->{'databases'} unless !($self->{'databases'});
    return 0;
}

=head2 B<next_db()>

Returns next database sorted by name. If end is reached 0 is returned.

After end was reached starts over at the next call of method.

=cut

sub next_db{
    my $self = shift;
    die "No databases set." unless $self->{'databases'}; 
    my @sorted = sort(keys(%{$self->{'databases'}}));
    if($self->{'index'} == scalar(@sorted)){
	$self->{'index'} = 0;
	return 0;
    }
    else{
	my $db = $self->{'databases'}->{$sorted[$self->{'index'}]};
	$self->{'index'}+=1;
	return $db;
    }
}


=head2 B<getDatabaseByFileName(string)>

Returns database hash of database that matches a particular tool-output-file

=cut

sub getDatabaseByFileName{
    my ($self,$name) = @_;
    my $h;
    foreach(keys(%{$self->{'databases'}})){
	if($name=~m/$_/i){
	    die "[ERROR] More than one matching database found for tool-file $name." unless !(ref($h));
	    $h = $self->{'databases'}->{$_};
	}
    }
    die "[ERROR] Unable to find matching database file for tool-file $name" unless ref($h);
    return $h;
}

=head1 INTERNAL METHODS

=head2 B<_getDBs()>

Returns hash-ref of all possible databases installed on this system.

hash-ref = {DB_NAME => {name => DB_NAME, path => DB_PATH, type =>DB_TYPE, format=>DB_FORMAT, taxon=>DB_TAXON }}

where

DB_NAME : name of the database, e.g. sprot_bacteria, pfam-a etc

DB_PATH : path to the database file(s)

DB_TYPE : sequence type of database, either nucleotide or protein

DB_FORMAT : format/tool the database was formated for, either hmm (HMMer) or blast (FASTA35, BLAST)

DB_TAXON : taxon of database, e.g. bacteria, fungi, viruses or complete

=cut

sub _getDBs{
    my %DBs = ();

    my $uniprotDir = DATABASE_PATH."/uniprot";
    my $pfamDir = DATABASE_PATH."/pfam";

# -- swissprot databases -- #
    # swissprot archaea 
    $DBs{'sprot_archaea'} = {name=>'sprot_archaea',
			      path=>$uniprotDir.'/uniprot_sprot_archaea.fas',
			      type=>'protein',
			      format=>'blast',
			      taxon=>'archaea'};
    # swissprot bacteria
    $DBs{'sprot_bacteria'} = {name=>'sprot_bacteria',
			      path=>$uniprotDir.'/uniprot_sprot_bacteria.fas',
			      type=>'protein',
			      format=>'blast',
			      taxon=>'bacteria'};

    # swissprot fungi
    $DBs{'sprot_fungi'} = {name=>'sprot_fungi',
			      path=>$uniprotDir.'/uniprot_sprot_fungi.fas',
			      type=>'protein',
			      format=>'blast',
			      taxon=>'fungi'};

    # swissprot invertebrates 
    $DBs{'sprot_invertebrates'} = {name=>'sprot_invertebrates',
			      path=>$uniprotDir.'/uniprot_sprot_invertebrates.fas',
			      type=>'protein',
			      format=>'blast',
			      taxon=>'invertebrates'};
    # swissprot mammals 
    $DBs{'sprot_mammals'} = {name=>'sprot_mammals',
			      path=>$uniprotDir.'/uniprot_sprot_mammals.fas',
			      type=>'protein',
			      format=>'blast',
			      taxon=>'mammals'};
    # swissprot plants 
    $DBs{'sprot_plants'} = {name=>'sprot_plants',
			      path=>$uniprotDir.'/uniprot_sprot_plants.fas',
			      type=>'protein',
			      format=>'blast',
			      taxon=>'plants'};
    # swissprot rodents 
    $DBs{'sprot_rodents'} = {name=>'sprot_rodents',
			      path=>$uniprotDir.'/uniprot_sprot_rodents.fas',
			      type=>'protein',
			      format=>'blast',
			      taxon=>'rodents'};
    # swissprot vertebrates 
    $DBs{'sprot_vertebrates'} = {name=>'sprot_vertebrates',
			      path=>$uniprotDir.'/uniprot_sprot_vertebrates.fas',
			      type=>'protein',
			      format=>'blast',
			      taxon=>'vertebrates'};
    # swissprot viruses 
    $DBs{'sprot_viruses'} = {name=>'sprot_viruses',
			      path=>$uniprotDir.'/uniprot_sprot_viruses.fas',
			      type=>'protein',
			      format=>'blast',
			      taxon=>'viruses'};
    # -- TrEmbl databases -- #
    # TrEmbl bacteria
    $DBs{'trembl_bacteria'} = {name=>'trembl_bacteria',
			      path=>$uniprotDir.'/uniprot_trembl_bacteria.fas',
			      type=>'protein',
			      format=>'blast',
			      taxon=>'bacteria'};

    # TrEmbl archaea 
    $DBs{'trembl_archaea'} = {name=>'trembl_archaea',
                              path=>$uniprotDir.'/uniprot_trembl_archaea.fas',
                              type=>'protein',
                              format=>'blast',
                              taxon=>'archaea'};

    # swissprot fungi
    $DBs{'trembl_fungi'} = {name=>'trembl_fungi',
                              path=>$uniprotDir.'/uniprot_trembl_fungi.fas',
                              type=>'protein',
                              format=>'blast',
                              taxon=>'fungi'};

    # swissprot invertebrates 
    $DBs{'trembl_invertebrates'} = {name=>'trembl_invertebrates',
                              path=>$uniprotDir.'/uniprot_trembl_invertebrates.fas',
                              type=>'protein',
                              format=>'blast',
                              taxon=>'invertebrates'};
    # swissprot mammals 
    $DBs{'trembl_mammals'} = {name=>'trembl_mammals',
                              path=>$uniprotDir.'/uniprot_trembl_mammals.fas',
                              type=>'protein',
                              format=>'blast',
                              taxon=>'mammals'};
    # swissprot plants 
    $DBs{'trembl_plants'} = {name=>'trembl_plants',
                              path=>$uniprotDir.'/uniprot_trembl_plants.fas',
                              type=>'protein',
                              format=>'blast',
                              taxon=>'plants'};
    # swissprot rodents 
    $DBs{'trembl_rodents'} = {name=>'trembl_rodents',
                              path=>$uniprotDir.'/uniprot_trembl_rodents.fas',
                              type=>'protein',
                              format=>'blast',
                              taxon=>'rodents'};
    # swissprot unclassified 
    $DBs{'trembl_viruses'} = {name=>'trembl_unclassified',
                              path=>$uniprotDir.'/uniprot_trembl_unclassified.fas',
                              type=>'protein',
                              format=>'blast',
                              taxon=>'unclassified'};
    # swissprot vertebrates 
    $DBs{'trembl_vertebrates'} = {name=>'trembl_vertebrates',
                              path=>$uniprotDir.'/uniprot_trembl_vertebrates.fas',
                              type=>'protein',
                              format=>'blast',
                              taxon=>'vertebrates'};
    # swissprot viruses 
    $DBs{'trembl_viruses'} = {name=>'trembl_viruses',
                              path=>$uniprotDir.'/uniprot_trembl_viruses.fas',
                              type=>'protein',
                              format=>'blast',
                              taxon=>'viruses'};

# -- HMM databases -- #
    # Pfam-A
    $DBs{'pfam-a'} = {name=>'Pfam-A',
		      path=>$pfamDir.'/Pfam-A.hmm',
		      type=>'protein',
		      format=>'hmm',
		      taxon=>'all'};
		      
    # Pfam-B
    $DBs{'pfam-b'} = {name=>'Pfam-B',
		      path=>$pfamDir.'/Pfam-B.hmm',
		      type=>'protein',
		      format=>'hmm',
		      taxon=>'all'};
    return \%DBs;
}

1;
