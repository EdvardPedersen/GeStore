package GePan::PipelineCreator;

use strict;
use Data::Dumper;
use GePan::Logger;

=head1 NAME

GePan::PipelineCreator

=head1 DESCRIPTION

Creates the pipeline from the user defined tools.

=head1 ATTRIBUTES

registered_tools: GePan::Collection::ToolConfig of all tools available

user_tools: array of tool names chosen by user

pipeline: created pipeline

logger: GePan::Logger object

=head1 CONSTRUCTOR

=head1 B<new()>

Returns an empty GePan::ToolConfig  object.

=cut

sub new{
    my $class = shift;
    my $self = {};
    return(bless($self,$class));
}


=head1 METHODS

=cut

=head2 B<createPipeline()>

Creates the pipeline for the current gepan run based on user defined tools.

=cut


sub createPipeline{
    my $self = shift;
    $self->{'logger'}->LogStatus("Creating pipeline for current gepan run.");
    $self->{'logger'}->LogError("PipelineCreator::createPipeline() - No Collection::ToolConfig set!") unless $self->{'registered_tools'};
    $self->{'logger'}->LogError("PipelineCreator::createPipeline() - No tools set by user!") unless $self->{'user_tools'};

    my %dependencies=(); 
    my $pipeline = [];

    my $collection = $self->{'registered_tools'};

    # create dependencies
    foreach my $tool1 (@{$self->{'user_tools'}}){
	$self->{'logger'}->LogError("PipelineCreator::createPipeline() - No user defined tool $tool1 found tool config!.") unless $collection->getElementByID($tool1);
	foreach my $tool2 (@{$self->{'user_tools'}}){

	    next unless $tool1 ne $tool2;

	    my $t1 = $collection->getElementByID($tool1);
	    my $t2 = $collection->getElementByID($tool2);

	    # check if tool1 is dependend on tool2
	    if(_checkDependency($self,$t1,$t2)){
		$dependencies{ $t1->{'id'}.":".$t2->{'id'}} = 1;
	    }
	    # check if tool2 is dependend on tool1
	    if(_checkDependency($self,$t2,$t1)){
		$dependencies{$t2->{'id'}.":".$t1->{'id'}} = 1;;
            }
	}
    }

    my @children = ();
    my %nodes = ();
    my %minimal = ();    

    # create tree for all tools except filters
    foreach my $k (keys(%dependencies)){
	my ($child,$parent) = split(":",$k);
	push @children,$child;
	$minimal{$_}{'name'} = $_ for $child,$parent;
	$minimal{$parent}{'after_that'}{$child} = $minimal{$child};
	$nodes{$_}{'tool_config'}=$collection->getElementByID($_) for $child,$parent;
	$nodes{$parent}{'children'}{$child} = $nodes{$child};
    }
    delete $nodes{$_} for @children;
    delete $minimal{$_} for @children;

    if(scalar(keys(%dependencies))<1){
	foreach my $tool (@{$self->{'user_tools'}}){
	    $nodes{$tool} = {tool_config=>$collection->getElementByID($tool)};	   
	    $minimal{$tool} = {name=>$tool}; 
	}
    }

    if(!(scalar(keys(%nodes)))){
	$self->{'logger'}->LogError("PipelineCreator::createPipeline() - no tools included in pipeline");
    }


    my @p = ();

    _flattenPipeline(\%nodes,\@p,0);

    # include filter
    $self->{'logger'}->LogWarning("PipelineCreator - inclusion of Filter not implemented!");   

    my $roots = join(",",(keys(%nodes)));
    $self->{'logger'}->LogWarning("PipelineCreator::createPipeline() - More than one root for pipeline found: $roots") unless scalar(keys(%nodes))==1; 

    $self->{'logger'}->LogStatus("Pipeline to be executed:\n".(Dumper %minimal));
    $self->{'pipeline'} = \@p; 
}



=head2 B<_flattenPipeline(ref)>

Takes pipeline tree and flattens it to a one dimensional array.

=cut

sub _flattenPipeline{
   my ($hash,$array,$depth) = @_;

    my @keys = keys(%$hash);

    foreach(@keys){
	if(ref($array->[$depth])){
	    push @{$array->[$depth]},$hash->{$_}->{'tool_config'};
	}
	else{
	    $array->[$depth] = [$hash->{$_}->{'tool_config'}];
	}
	if($hash->{$_}->{'children'}){
	    _flattenPipeline($hash->{$_}->{'children'},$array,$depth+1);
	}
    }
}


=head1 GETTER & SETTER METHODS

=head2 B<setRegisteredTools(GePan::Collection::ToolConfig)>

Sets the collection of all tools available.

=cut

sub setRegisteredTools{
    my ($self,$collection) = @_;
    $self->{'registered_tools'} = $collection;
}


=head2 B<getRegisteredTools()>

Returns GePan::Collection::ToolConfig of all registered tools.

=cut

sub getRegisteredTools{
    my $self = shift;
    return $self->{'registered_tools'};
}


=head2 B<setUserTools(array-ref)>

Sets list of all userdefined tools

=cut

sub setUserTools{
    my ($self,$ref) = @_;
    $self->{'user_tools'} = $ref;
}


=head2 B<getUserTools()>

Returns array of userdefined tools

=cut

sub getUserTools{
    my $self = shift;
    return $self->{'user_tools'};
}


=head2 B<setPipeline(pipeline).

Sets configered pipeline of gepan run.

=cut

sub setPipeline{
    my ($self,$ref) = @_;
    $self->{'pipeline'} = $ref;
}


=head2 B<getPipeline()>

Returns configured pipeline.

=cut

sub getPipeline{
    my $self = shift;
    return $self->{'pipeline'};
}


=head2 B<setParams(ref)>

Sets attributes of given hash to given values.

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



=head1 INTERNAL METHODS

=head2 B<_checkDependencies(GePan::ToolConfig t1,GePanToolConfig t2)>

Checks if t1 is dependend on t2. Returns 0 if not, 1 otherwise.

=cut

sub _checkDependency{
    my ($self,$t1,$t2) = @_;
    
    # get input types of both tools
    my $i1 = $t1->{'input_type'};

    # get input_sequence_type of both tools
    my $is1  = $t1->{'input_sequence_type'};

    # get output sequence type of both
    return 0 unless (defined($t2->{'output_sequence_type'}));
    my $os2 = $t2->{'output_sequence_type'};

    # get output type of both
    return 0 unless (defined($t2->{'output_type'}));
    my @o2 = split(",",$t2->{'output_type'});
    
    # check if any of the output sequence types of tool2 are input of tool1
    my $check = 0;
    foreach my $a (@o2){
	$check++ unless ($a ne $i1);
    }

    if(($check)&&($is1 eq $os2)){
	return 1;
    }
    else{
	return 0;
    }
}



1;
