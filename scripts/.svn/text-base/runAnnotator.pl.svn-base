
use strict;
use Getopt::Std;
use GePan::Annotator;
use GePan::Logger;
use GePan::Collection::Sequence;
use GePan::ToolRegister;
use XML::Simple;
use Data::Dumper;
use GePan::Parser::Input::Fasta;
use GePan::Exporter::Embl;
use GePan::Exporter::XML::SimpleAnnotation;
use GePan::Config qw(GEPAN_PATH);
=head1 NAME

    runAnnotator.pl

=head1 DESCRIPTION

Script annotates all sequences and exports it regarding the given exporter type.

=head1 PARAMETER

p: path to node working directory

t: SGE_TASK_ID

=cut


eval{
    _main();
};
if($@){
    print $@;
}


sub _main{
    our %opts;
    getopts("p:t:",\%opts);

    my $paramFile = $opts{'p'}."/parameter.xml";
    my $params = _createParams($paramFile);
    $params->{'node_dir'} = $opts{'p'};
    $params->{'task_id'} = $opts{'t'};

    # create logger
    my $logger = GePan::Logger->new();
    $logger->setStatusLog("gepan.log");
    $logger->setNoPrint(1);
    $params->{'logger'} = $logger;

    # register all known tools of pipeline
    $params->{'tool_register'} = _registerTools($params);

    # register all known databases
    $params->{'db_register'} = _registerDBs($params);

    $params->{'sequences'} = _runAnnotator($params);

    _runExporter($params);
}



=head2 B<_runExporter()>

Writes out a dump of GePan::Collection::Sequence of sequences annotated on this node.

=cut

sub _runExporter{
    my $params = shift;

    # create path to dump file
    my $dumpPath = $params->{'node_dir'}."/results/result.".$params->{'task_id'}.".dump";

    # 
    open(OUT,">$dumpPath") or die "Failed to open dump output file $dumpPath for writing.";
    my $collection = $params->{'sequences'};
    my $dump = Data::Dumper->new([$collection],[qw($collection)])->Purity(1)->Dump();
    print OUT $dump;
    close(OUT);
}



=head2 B<_registerDBs()>

Reads in all DatabaseDefinition files and creates DatabaseRegister Objects.

=cut

sub _registerDBs{
    my $params= shift;

    # register all defined databases
    $params->{'logger'}->LogStatus("Registering databases");
    my $databaseRegister = GePan::DatabaseRegister->new();
    my $p = {'logger'=>$params->{'logger'}};
    if(!(ref($params->{'db_def_dir'}))){
        $p->{'config_dir'} = $params->{'db_def_dir'};
    }
    else{
        $p->{'config_dir'} = GEPAN_PATH."/GePan/DatabaseDefinitions";
    }
    $databaseRegister->setParams($p);
    $databaseRegister->register();
    return $databaseRegister;
}


sub _getCollection{
    my ($params,$types) = @_;

    my $seqs = GePan::Collection::Sequence->new();
    foreach my $t (@$types){
	my $collection;
	my $xmlPath = $params->{'node_dir'}."/data/$t/collection.xml";
	my $parser = XML::Simple->new();
        my $data = $parser->XMLin($xmlPath);
	eval($data);
	while(my $s = $collection->getNextElement()){
	    $seqs->addElement($s);
	}
    }

    return $seqs;
}



sub _getTaskSequences{
    my ($params,$collection) = @_;

    my $id = $params->{'task_id'};
    my $path = $params->{'node_dir'}."/data/cds";
    opendir(DIR,$path);
    my @files = grep{$_=~/.*\.$id/}readdir(DIR);
    closedir(DIR);

    die "More or less than one file found" unless scalar(@files)==1;

    my $parser = GePan::Parser::Input::Fasta->new();
    $parser->setParams({logger=>$params->{'logger'},
			file=>$params->{'node_dir'}."/data/cds/".$files[0],
			type=>'cds'});
    $parser->parseFile();
    my $task = $parser->getCollection();

    my $seqs = GePan::Collection::Sequence->new();
    while(my $seq = $task->getNextElement()){
	if($collection->getElementByID($seq->getID())){
	    $seqs->addElement($collection->getElementByID($seq->getID()));
	}
    }
    return $seqs;
}



=head2 B<_runAnnotator()>

Runs the annotation for all sequences and all tools.

=cut

sub _runAnnotator{
    my $params = shift;


    # get all annotation tools
    my $allAnnotationTools = $params->{'tool_register'}->getCollection()->getElementsByAttributeHash({type=>'annotation'});

    # list of all input types for tools
    my $toolInputSequenceTypes = {};

    
    # get tools that are defined by user
    my $annotationTools = GePan::Collection::ToolConfig->new();
    while(my $config = $allAnnotationTools->getNextElement()){
	my $name = $config->getID();
	if($params->{'tool_string'}=~m/$name/){
	    $annotationTools->addElement($config);
	    $toolInputSequenceTypes->{$config->getInputSequenceType()} = 1;
	}
    }

    # get attribute collection
    my $aConfigs_tmp = $params->{'tool_register'}->getCollection()->getElementsByAttributeHash({type=>'prediction',sub_type=>'attribute',input_sequence_type=>'cds'});
    my $aConfigs = GePan::Collection::ToolConfig->new();
    while(my $config = $aConfigs_tmp->getNextElement()){
	$aConfigs->addElement($config) if $params->{'tools'}->{$config->getID()};
	$toolInputSequenceTypes->{$config->getInputSequenceType()} = 1;
    }

    # get collection of sequences to annotate
    my @tIST = (keys(%$toolInputSequenceTypes));
    my $allSeqs = _getCollection($params,\@tIST);

    # get just those sequences of one task
    my $seqs = _getTaskSequences($params,$allSeqs);


    # createAnnotator object
    my $annotator = GePan::Annotator->new();
    $annotator->setParams({logger=>$params->{'logger'},
                           work_dir=>$params->{'node_dir'},
                           annotation_tools=>$annotationTools,
                           attribute_tools=>$aConfigs,
                           sequences=>$seqs,
                           type=>'cds',
                           database_collection=>$params->{'db_register'}->getCollection()});
    $annotator->annotate();

    return $seqs;
}


=head2 B<_createToolHash(string)>

Creates a hash-ref from given tool-string.

Hash is of form

     {tool_name=>{parameter_name=>parameter_value}}

=cut

sub _createToolHash{
    my ($string,$logger) = @_;
    $string=~s/\s//g;


    # split at ";" i.e. one line per tool
    my @tools = split(/\;/,$string);
    my $toolHash = {};

    # for each tool-line
    foreach my $toolString (@tools){
        # split at ":" i.e. between tool_name and parameter string
        my @toolSplit = split(/\:/,$toolString);
        $logger->LogError("runAnnotator::_createToolHash - Wrong number of elements in split of tool string.") unless scalar(@toolSplit)<=2;
        my $toolName = $toolSplit[0];

        my $h = {};
        $h->{'id'} = $toolName;
        
        if(scalar(@toolSplit)==1){ 
            $toolHash->{$toolName} = $h;
        }
        else{
            # split at "," i.e. one line per parater=>value pair
            my @parameterStrings = split(/\,/,$toolSplit[1]);
            
            # foreach parameter=>value pair
            foreach my $parameterString (@parameterStrings){
                # split at "=" i.e. seperate parameter_name from parameter_value
                my @parameterSplit = split(/\=/,$parameterString);
                $logger->LogError("startStandAlong::_createToolHash() - Number of elements > 2 for parameter split.") unless ((scalar(@parameterSplit))&&(scalar(@parameterSplit)<=2));
                
                if($parameterSplit[1]){
                    $h->{$parameterSplit[0]} = $parameterSplit[1];
                }
                else{
                    $h->{$parameterSplit[0]} = "";
                }
            }
            $toolHash->{$toolName} = $h;
        }
    }
    return $toolHash;
}


=head2 B<_registerTools()>

Reads in all ToolDefinition files and creates ToolRegister Object.

=cut

sub _registerTools{
    my $params= shift;

    # create hash-ref for tools of form {toolName=>{parameter_name=>$parameter_value}}
    $params->{'tools'} = _createToolHash($params->{'tool_string'},$params->{'logger'});

    # register all defined tools
    my $toolRegister = GePan::ToolRegister->new();
    my $p = {'logger'=>$params->{'logger'}};
    if(!(ref($params->{'tool_def_dir'}))){
        $p->{'config_dir'} = $params->{'tool_def_dir'};
    }
    else{
        $p->{'config_dir'} = GEPAN_PATH."/GePan/ToolDefinitions";
    }
    $toolRegister->setParams($p);
    $toolRegister->register();
    return $toolRegister;
}

=head2 B<_createParams(string)>

Creates Parameter hash from given parameter string.

=cut

sub _createParams{
    my $file = shift;
    my $parser = XML::Simple->new();
    my $data = $parser->XMLin($file);
    return $data;
}



