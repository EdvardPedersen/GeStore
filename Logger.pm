package GePan::Logger;

use strict;
use Data::Dumper;


=head1 NAME

GePan::Logger

=head1 DESCRIPTION

Class for logging status- as well as error messages or warning to logg file

=head1 ATTRIBUTES

status_log: path to log-file for status messages

no_print: if set messages are just printed to STDOUT but not to log-file

=head1 CONSTRUCTOR

=head1 B<new()>

Returns an empty GePan::Logger object.

=cut

sub new{
    my $class = shift;
    my $self = {};
    return(bless($self,$class));
}

=head1 METHODS

=cut

=head2 LogStatus(string)

Writes given string to log file

=cut

sub LogStatus{
    my ($self,$string) = @_;
    if(!($self->{'no_print'})){
	$self->_createLogFile();
	open(LOG,">>".$self->{'status_log'}) or die "Failed to open status log file for writing.";
	print LOG "\n[STATUS] $string\n";
	close(LOG);
    }
    print STDOUT "\n[STATUS] $string\n";
}


=head2 LogError(string)

Writes given string to log file and exits gepan.

=cut

sub LogError{
    my ($self,$string) = @_;
    if(!($self->{'no_print'})){
	$self->_createLogFile();
	open(LOG,">>".$self->{'status_log'}) or die "Failed to open log file for writing.";
	print LOG "\n## [ERROR] $string\n";
	close(LOG);
    }
    die "\n[ERROR] $string\n\n";
}


=head2 LogWarning(string)

Writes warning to log file

=cut

sub LogWarning{
    my ($self,$string) = @_;
    if(!($self->{'no_print'})){
	$self->_createLogFile();
	open(LOG,">>".$self->{'status_log'}) or die "Failed to open log file for writing." ;
	print LOG "\n[WARNING] $string\n";
	close(LOG);
    }
    else{
	print STDOUT $string;
    }
}


=head1 GETTER & SETTER METHODS

=head2 B<setStatusLog(path)>

Sets path to log file

=cut

sub setStatusLog{
    my ($self,$path) = @_;
    $self->{'status_log'} = $path;
}


=head2 B<getStatusLog()>

Returns path to log file

=cut

sub getStatusLog{
    my $self = shift;
    return $self->{'status_log'};
}   


=head2 B<setNoPrint(int)>

Set no_print.

=cut

sub setNoPrint{
    my ($self,$int) = @_;
    $self->{'no_print'} = $int;
}


=head2 B<getNoPrint(int)>

Returns no_print.

=cut

sub getNoPrint{
    my $self = shift;
    return $self->{'no_print'};
}


=head1 INTERNAL METHODS

=cut

=head2 B<_createLogFile>

Creates logfile if not existing.

=cut

sub _createLogFile{
    my $self = shift;
    die "No path to log file given" unless $self->{'status_log'};
    if(!(-f $self->{'status_log'})){
	my $time = time;
	my $file = $self->{'status_log'};
	open(LOG,">$file") or die "Failed to open log file for writing.";
	print LOG "****** Log file for status, error and warning messages for gepan run date $time ********\n\n";
	close(LOG);
    }
}


1;
