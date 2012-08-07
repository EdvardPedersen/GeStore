package GePan::Mapping::Mapping;
use strict;
use Data::Dumper;
use GePan::Logger;
use GePan::Config qw(GEPAN_PATH);
=head1 NAME

GePan::Mapping

=head1 DESCRIPTION

Super-class of direct mapping of hits to information charts


=head1 CONSTRUCTOR

=head2 B<new()>

Returns an empty Mapping object

=cut

sub new{
    my $class = shift;
    my $self = {};
    return (bless($self,$class));
}

=head1 Indexing Methods

=head2 B<indexPfam2GO()>
=head2 B<indexGO2EC()>

Creates lookup tables from stored convertion charts

=cut

sub indexPfam2GO{
	my $self = shift;
	my $lookup = {};
	open(FILEHANDLE, GEPAN_PATH."/GePan/MappingFiles/pfam2go.txt") or $self->{'logger'}->LogError("GePan::Mapping::indexPfam2GO() - Can't find mapping file");
	while (<FILEHANDLE>){
		#Skips header lines
		if ($_ =~ /^\!/){next;}
		
		else{
			#Sorts PF##### and GO:####### into $start and $end
			my @data = split (' ', $_);
			my $end = pop(@data);
			my $tempstart = shift(@data);
			my $start = substr($tempstart,5,7);
		
			if (ref($lookup->{$start})){
				push @{$lookup->{$start}},$end;
			}
			else{
				$lookup->{$start} = [$end];
			}
		}
	}
	$self->{'Pfam2GO'} = $lookup;
}

sub indexGO2EC{
	my $self = shift;
	my $lookup = {};
	open (FILEHANDLE, GEPAN_PATH."/GePan/MappingFiles/ec2go.txt") or $self->{'Logger'}->LogError("GePan::Mapping::indexGO2EC() - Can't find mapping file");
	while (<FILEHANDLE>){
		#Skips header lines
		if ($_ =~ /^\!/){next;}
		
		else{
			#Sorts EC and GO:####### into @data and $go
			my @data = split (' ', $_);
			my $go = pop(@data);
			my $tempec = shift(@data);
			my $ec = substr($tempec,3);
			
			if (ref($lookup->{$go})){
				push @{$lookup->{$go}},$ec;
			}
			else{
				$lookup->{$go} = $ec;
			}
		}
	}
	$self->{'GO2EC'} = $lookup;
}

=head1 Getter Methods

=head2 B<getPfam2GO()>
=head2 B<getGO2EC()>

Pushes a Pfam/GO hit to get GO/EC information.

=cut

sub getPfam2GO{
	my ($self, $pfam) = @_;
	if ($pfam =~ m/\./){
		my @temp = split(/\./, $pfam);
		$self->{'logger'}->LogWarning("Error in getPfam2GO") unless scalar(@temp)>1;
		$pfam = $temp[0];
	}
	return $self -> {'Pfam2GO'} -> {$pfam};


}

sub getGO2EC{
	my ($self, $go) = @_;
	my $result = [];
	foreach (@$go){
		push (@$result, $self->{'GO2EC'}->{$_});
	}
	return $result;


}

1;
