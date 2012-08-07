package GePan::Tool::Prediction::Signalp;
use base qw(GePan::Tool);

use Data::Dumper;
use GePan::Config qw(SIGNALP_PATH);
use GePan::Logger;


=head1 NAME

GePan::Tool::Prediction::Signalp

=head1 DESCRIPTION

Class for executing SignalP prediction tool.

Additional parameter (gram+, gram-, euk) have to passed through by the start script.

Sub-class of GePan::Tool

=head1 METHODS

=head2 B<execute()>

Exectures signalp. Output is written to given output directory.

Output is written to given output dir.

=cut

sub execute{
    my $self = shift;

    my $statement = $self->_getExecuteStatement(); 
    $self->{'logger'}->LogStatus("SignalP execute statement: $statement");
    my $exit = system($statement);
    $self->{'logger'}->LogError("GePan::Tool::Prediction::Signalp::execute - executing SignalP failed") if ($exit);
}

=head2 B<getToolName()>

Returns short name of tool (\'glimmer\')

=cut

sub getToolName{
    my $self = shift;
    return "signalp";
}

=head1 INTERNAL METHODS

=cut

=head2 B<_getExecuteStatement()>

Creates and returns the execute statement for glimmer.

=cut

sub _getExecuteStatement{
    my $self = shift;

my $outputPath = $self->{'output_dir'}.'/'.$self->{'output_file'};

    my @split = split(/\./,($self->{'output_file'}));
    my $last = pop(@split);
    my $tag = join(".",@split);
    if($last ne 'predict'){
        $tag .= $last;
    }

    $self->{'logger'}->LogError("GePan::Tool::Prediction::Signalp::execute - No mandatory parameter \'t\' given!") unless ($self->{'parameter'}->{'t'});

    my $parameter_string = "";
    foreach(keys(%{$self->{'parameter'}})){
        $parameter_string.="-".$_." ".$self->{'parameter'}->{$_}." ";
    }

    my $statement = SIGNALP_PATH." $parameter_string -f short ".$self->{'input_file'}." > $outputPath";
    return $statement;
}




1;
