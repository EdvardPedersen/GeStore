package GePan::ToolRegister;

use strict;
use GePan::ToolConfig;
use GePan::ToolConfig::Annotation;
use GePan::ToolConfig::Prediction;
use GePan::ToolConfig::Filter;
use Data::Dumper;
use XML::Simple;
use GePan::Logger;
use GePan::Collection::ToolConfig;

=head1 NAME

GePan::ToolRegister

=head1 DESCRIPTION

Class reads in all configure files for all tools and creates ToolConfig-objects for each tool.

=head1 ATTRIBUTES

config_dir: directory of tool-config files

logger: GePan::Logger object

collection: GePan::Collection::ToolConfig object of all registered tools

=head1 CONSTRUCTOR

=head1 B<new()>

Returns an empty GePan::ToolRegister  object.

=cut


sub new{
    my $class = shift;
    my $self = {};
    return (bless($self,$class));
}


=head2 B<register()>

Reads in all config files and registeres the specific tools

=cut

sub register{
    my $self =  shift;

    my $dir = $self->{'config_dir'}?$self->{'config_dir'}:"../ToolDefinitions/";
    $self->{'logger'}->LogError("ToolRegister::register() - Directory $dir of tool definition files does not exist.") unless -d $dir;

    opendir(DIR,$dir) or $self->{'logger'}->LogError("ToolRegister::register() - Failed to open directory $dir for reading");
    my @files = grep {(($_=~/^.*\.xml$/)&&(-f "$dir/$_"))}readdir(DIR);

    my $collection = GePan::Collection::ToolConfig->new();
    $self->{'collection'} = $collection;

    foreach(@files){
	my $path = $self->{'config_dir'}."/".$_;
        $collection->addElement(_parseFile($self,$path));
    }
}


=head1 INTERNAL METHODS

=head2 B<_parseFile(PATH)>

Reads in file PATH and returns a GePan::ToolConfig object;

=cut

sub _parseFile{
    my ($self,$path) = @_;
    my $parser = XML::Simple->new();
    my $data = $parser->XMLin($path);
    my $config;
    if(lc($data->{'type'}) eq 'annotation'){
	$config = GePan::ToolConfig::Annotation->new();
    }
    elsif(lc($data->{'type'}) eq 'prediction'){
	$config = GePan::ToolConfig::Prediction->new();
    }	
    elsif(lc($data->{'type'}) eq 'filter'){
	$config = GePan::ToolConfig::Filter->new();
    }
    else{
	die "Unknown ToolConfig type!";
    }
    $config->setParams($data);
#    $self->{'logger'}->LogStatus("\n\n----------------------$path");
#    $self->{'logger'}->LogStatus(Dumper($config));
    return $config;
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


=head2 B<setParams(ref)>

Sets all parameter given in hash to given values.

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
