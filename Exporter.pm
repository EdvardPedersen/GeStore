package GePan::Exporter;

use strict;
use Data::Dumper;
use GePan::Config qw(GEPAN_PATH);
=head1 NAME

GePan::Exporter

=head1 DESCRIPTION

Base-class for all exporter.

=head1 ATTRIBUTES

output_directory: path of the directory the file files should be written to

file: name of the output file

logger: GePan::Logger object

=head1 CONSTRUCTOR

=head2 B<new()>

Returns an empty GePan::Exporter object

=cut

sub new{
    my $class= shift;
    my $self = {};
    _load();
    return (bless($self,$class));
}

=head1 METHODS

=head2 B<export()> 

Abstract method. Has to be implemented by all sub-classes

=cut

sub export{
    my $self = shift;
    $self->{'logger'}->LogError("GePan::Exporter::export() - Abstract method GePan::Export->export not implemented by syb-class");
}


=head1 GETTER & SETTER METHODS

=head2 B<setParams(hash-ref)>

Sets all attributes by hash of form {attribute_name=>attribute_value} 

=cut

sub setParams{
    my ($self,$hash) = @_;
    foreach(keys(%$hash)){
        $self->{$_} = $hash->{$_};
    }
}

=head2 B<setOutputDir(output_directory)>

Sets the output directory to output_directory.

=cut

sub setOutputDir{
    my ($self,$dir) = @_;
    $self->{'output_directory'} = $dir;
}


=head2 B<getOutputDir()>

Returns the output directory.

=cut

sub getOutputDir{
    my $self = shift;
    return $self->{'output_directory'};
}

=head2 B<setFile(string)>

Sets name of the result-file.

=cut

sub setFile{
    my ($self,$name) = @_;
    $self->{'file'} = $name;
}

=head2 B<getFile()>

Returns name of the output file.

=cut

sub getFile{
    my $self = shift;
    return $self->{'file'};
}


=head2 B<setLogger(GePan::Logger)>

Sets GePan::Logger object.

=cut

sub setLogger{
    my ($self,$l) = @_;
    $self->{'logger'} = $l;
}

=head2 B<_load()>

Loads required classes for this package.

=cut

sub _load(){

    # load hit classes
    my $hitDir = GEPAN_PATH."/GePan/Hit";
    opendir(DIR,$hitDir);
    my @classes = grep{$_=~/.*\.pm/}readdir(DIR);
    closedir(DIR);
    foreach(@classes){
        my $class = $hitDir."/$_";
        eval{_requireClass($class)};
        die $@ if $@;
    }

}

=head2 B<_requireClass(string)>

Loads class of name string.
=cut

sub _requireClass{
    my $class = shift;
    require $class;
}



1;
