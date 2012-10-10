package GePan::DatabaseRegister;

use strict;
use Data::Dumper;
use GePan::Config qw(DATABASE_PATH);
use GePan::DatabaseConfig;
use GePan::Collection::DatabaseConfig;
use GePan::Logger;

=head1 NAME

GePan::DatabaseRegister

=head1 DESCRIPTION

Class for registering all databases available.

=head1 ATTRIBUTES

config_dir: directory of configuration files of databases

collection: GePan::Collection::DatabaseConfig object of all registered databases.

logger:  GePan::Logger object

=head1 CONSTRUCTOR

=head2 B<new()>

Returns new GePan::Databases object;

=cut

sub new{
    my $class = shift;
    my $self = {};
    return (bless($self,$class));
}

=head1 METHODS

=head2 B<register()>

Reads all database configuration files and creates GePan::Collection::DatabaseConfig object of all databases.

=cut

sub register{
    my $self = shift;
    $self->{'logger'}->LogError("DatabaseRegister::register() - No path to directory of configuration files set.") unless $self->{'config_dir'};
    
    opendir(DIR,$self->{'config_dir'}) or $self->{'logger'}->LogError("DatabaseRegister::register() - Failed to open directory ".$self->{'config_dir'}." for reading");
    my @files = grep {(($_=~/^.*\.conf$/)&&(-f $self->{'config_dir'}."/$_"))}readdir(DIR);

    my $collection = GePan::Collection::DatabaseConfig->new();
    $collection->setLogger($self->{'logger'});

    foreach(@files){
        my $path = $self->{'config_dir'}."/".$_;
        $collection->addElement(_parseFile($path));
    }
    $self->{'collection'} = $collection;
}


=head1 GETTER & SETTER METHODS


=head2 B<getCollection()>

Returns GePan::Collection::ToolConfig of all registered tools.

=cut

sub getCollection{
    my $self = shift;
    return $self->{'collection'};
}

=head2 B<setConfigDir(string)>

Sets the directory of tool-config files.

=cut

sub setConfigDir{
    my ($self,$dir) = @_;
    $self->{'config_dir'} = $dir;
}

=head2 B<getConfigDir()>

Returns the directory of tool-config files.

=cut

sub getConfigDir{
    my $self = shift;
    return $self->{'config_dir'};
}

=head2 B<setLogger(GePan::Logger)>

Sets GePan::Logger object.

=cut

sub setLogger{
    my ($self,$l) = @_;
    $self->{'logger'} = $l;
}


=head1 INTERNAL METHODS

=head2 B<_parseFile(PATH)>

Reads in file PATH and returns a GePan::DatabaseConfig object;

=cut

sub _parseFile{
    my $path = shift;
    my $config = GePan::DatabaseConfig->new();
    my $parser = XML::Simple->new();
    my $data = $parser->XMLin($path);
    $config->setParams($data);
    my $dbPath = DATABASE_PATH."/".$config->getPath();
    $dbPath=~s/\/\//\//g;
    $config->setPath($dbPath);
    return $config;
}

=head2 B<setParams(ref)>

Sets all keys of given hash as parameter to given value.

=cut

sub setParams{
    my ($self,$h) = @_;
    foreach(keys(%$h)){
	$self->{$_} = $h->{$_};
    }
}

1;
