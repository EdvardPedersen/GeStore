#!/usr/bin/perl -w 

use strict;
#use lib '/home/tim/';
use Data::Dumper;
use Getopt::Std;
use GePan::Annotator;
use GePan::FileScheduler;
use GePan::Tool::Prediction::Mga;
use GePan::Parser::Input::Fasta;
use GePan::Exporter::Embl;
use GePan::Exporter::Fasta;
use GePan::Exporter::XML::SimpleAnnotation;
use GePan::PipelineCreator;
use GePan::Tool::Annotation::Pfam;
use GePan::Tool::Annotation::Blast;
use GePan::Tool::Annotation::Fasta;
use GePan::Tool::Prediction::Glimmer3;
use GePan::ToolRegister;
use GePan::Logger;
use GePan::ClassConfigurator;

=head1 NAME

startStandAlone.pl

=head1 DESCRIPTION

Script to start the GePan pipeline standalone on a usual PC without queueing system.

=head1 PARAMETER:

w:  working directory. All temporary files and result files are stored here.

f:  fasta file of contigs or sequences

p: String of tools that should be run. Additional parameters can be set, too. String is of form:

    "TOOLNAME1: parameter_name1=parameter_value, parameter_name2=parameter_value; TOOLNAME2: parameter_value1= ...."

T:  Type of input sequences

    p = protein

    n = nucleotide

S: Type of input sequences (e.g. contig or cds)

t:  list of taxa separated by ',', e.g. "fungi,archaea".Just given taxon databases are searched.

c:  Number of CPU/Cores used for database searches (for stand alone version)

o:  Output generator of choice
    
    1 = Complete (human readable) output tab separated (GePan::Exporter::CompleteTabSeparated)

    2 = Functional (human readable) output tab separated (GePan::Exporter::FunctionalTabSeparated)

    3 = Simple output in xml-format (GePan::Exporter::XML::SimpleAnnotation)

    4 = Complete project output in xml-format (GePan::Exporter::XML::Project)

r: Just annotator is run on old working directory

=cut


eval{
    _main();
};

if($@){
    print $@;
}



=head1 METHODS

=head2 B<_main()>

Main method. Gets command-line parameters, creates run directory and calls all subs.

=cut

sub _main{
    
    # get command-line parameter
    our %opts;
    getopts("b:d:w:f:p:t:c:T:S:o:r:",\%opts);

    my $params = {work_dir => $opts{'w'},
                  fasta => $opts{'f'},
                  tool_string => $opts{'p'},
                  taxa => $opts{'t'}?$opts{'t'}:0,
                  cpus  => $opts{'c'}?$opts{'c'}:1,
                  sequence_input_type => $opts{'S'},
                  exporter_type => $opts{'o'},
                  old_work_dir => $opts{'r'},
		  sequence_type => $opts{'T'},
		  tool_def_dir=>$opts{'d'},
		  db_def_dir=>$opts{'b'}};

    # check given parameter
    _checkScriptParameter($params);


    # create Logger
    my $logger = GePan::Logger->new();
    $logger->setStatusLog(($params->{'work_dir'})."/gepan.log");
    $params->{'logger'} = $logger; 

    # get starting time for run-time estimation   
    $params->{'running_since'} = time;
    $logger->LogStatus("GePan started at ".$params->{'running_since'});

    # register all known tools of pipeline
    $params->{'tool_register'} = _registerTools($params);

    # check that all user_defined tools are actually registered
    _checkRegistered($params);

    # register all known databases
    $params->{'db_register'} = _registerDBs($params);

    $logger->LogError("startStandAlone::_main() - No tools registered") unless ($params->{'tool_register'}->getCollection()->getSize());

    # if old working directory is given just 
    # start annotator and skip rest.
    if($params->{'old_work_dir'}){
        _rerunAnnotation($params);
    }

    # creates the pipeline, directory structure etc
    _prepareGePan($params);

    # get ToolConfigurator
    $params->{"configurator"} = GePan::ClassConfigurator->new();
    $params->{"configurator"}->setParams({work_dir=>$params->{'work_dir'},
					  logger=>$params->{'logger'},
					  tool_files_dir=>$params->{'tool_files_dir'},
					  tool_parameter=>$params->{'tools'},
					  cpu=>$params->{'cpus'},
					  data_files_dir=>$params->{'data_files_dir'}});

    # run the pipeline
    $params->{'logger'}->LogStatus("Running pipeline");
    runPipeline($params);

    # run annotator
    $params->{'logger'}->LogStatus("Running annotator");
    $params->{'sequences'} = _runAnnotator($params);

    # run exporter
    $params->{'logger'}->LogStatus("Running exporter");
    _runExporter($params);

}









=head2 B<_checkRegistered()>

Check method to determine that all user defined tools are actualy registered

=cut

sub _checkRegistered{
    my $params = shift;
   
    foreach(keys(%{$params->{'tools'}})){
	my $tool = $params->{'tool_register'}->getCollection()->getElementByID($_); 
	$params->{'logger'}->LogError("startStandAlone::_checkRegistered():: No tool with ID $_ found in registered tools.")  unless $tool;
    }
}




=head2 B<_runExporter()>

Exports annotations with given exporter type:

1: Sequences are exported in embl format. If contigs are given one Embl file per contig is written out. 

2: Simple XML output of all sequences with annotations and best hit is exported

=cut

sub _runExporter{
    my $params = shift;


    my $exporter;
    if($params->{'exporter_type'} eq '2'){
	$exporter = GePan::Exporter::XML::SimpleAnnotation->new();
	my $exporterParams = {"output_directory"=>$params->{'result_files_dir'},
			       file=>"simpleOutput.xml",
			       logger=>$params->{'logger'}};

	my $cds = GePan::Collection::Sequence->new();
	while(my $seq = $params->{'sequences'}->getNextElement()){
	    if(($seq->getType() eq "cds")&&($seq->getSequenceType() eq "nucleotide")){
		$cds->addElement($seq);
	    }
	}
	
	$params->{'logger'}->LogError("No cds sequences found.") unless $cds->getSize();

	$exporterParams->{'collection'} = $params->{'sequences'}; 

	$exporter->setParams($exporterParams);
	$exporter->export;	
    }
    elsif(($params->{'exporter_type'} eq "1a")||($params->{'exporter_type'} eq "1b")){
	# get input contig or read sequences
	my $contigs;
	if(-d ($params->{'data_files_dir'}."/contig")){
	    my $path = $params->{'data_files_dir'}."/contig/nucleotide";
	    opendir(IN,$path) or $params->{'logger'}->LogError("startStandAlone::_runExporter() - Failed to open directory $path fo reading.");
	    my @files = grep {-f "$path/$_"}readdir(IN);
	    $params->{'logger'}->LogError("startStandAlone::_runExporter() - No contig/read files found in directory $path") unless scalar(@files);
	    my $parser = GePan::Parser::Input::Fasta->new();
	    $path.="/".$files[0];
	    $params->{'logger'}->LogError("startStandAlone::_runExporter() - No contig/read files found $path") unless (-f($path));
	    $parser->setParams({file=>$path,
				logger=>$params->{'logger'},
	    			type=>'contig'});
	    $parser->parseFile();
	    $contigs = $parser->getCollection();
	}
	elsif(-d ($params->{'data_files_dir'}."/read")){
	    my $path = $params->{'data_files_dir'}."/read/nucleotide";
            opendir(IN,$path) or $params->{'logger'}->LogError("startStandAlone::_runExporter() - Failed to open directory $path fo reading.");
            my @files = grep {-f "$path/contig"}readdir(IN);
            my $parser = GePan::Parser::Fasta->new();
            $parser->setParams({file=>"$path/read/".$files[0],
                                logger=>$params->{'logger'},
                                type=>'read'});
            $contigs = $parser->getCollection();
	}

	$exporter = GePan::Exporter::Embl->new();
	$exporter->setParams({collection=>$params->{'sequences'},
			     parent_collection=>$contigs,
			     logger=>$params->{'logger'},
			     output_directory=>$params->{'work_dir'}."/results"});
	if($params->{'exporter_type'} eq "1a"){
	    $exporter->setStrict(1);    
	}	
	$exporter->export();
    }
    else{
	$params->{'logger'}->LogError("startStandAlone::_runExporter() - Unknown exporter type ".$params->{'exporter_type'});
    }

}



=head2 B<_runAnnotator()>

Runs the annotation for all sequences and all tools.

=cut

sub _runAnnotator{
    my $params = shift;

    my $sequences = {};

    my $seqs = _createSequences($params);

    # get annotation tool input types, e.g. data types that have to be annotated
    my @userTools = (keys(%{$params->{'tools'}}));
    my $allTools = $params->{'tool_register'}->getCollection()->getElementsByAttributeHash({type=>'annotation'});
    my $annotationTools = GePan::Collection::ToolConfig->new();
    my $types = {};
    foreach my $ut (@userTools){
	next unless ($allTools->getElementByID($ut));
	my $config = $allTools->getElementByID($ut);
	$types->{$config->getInputSequenceType().":".$config->getInputType()} = 1;
	$annotationTools->addElement($config);
    }
  
    # create pathes to files of datatypes
    my $paths = {};
    foreach my $dt (keys(%$types)){
	my ($inputSequenceType,$inputType) = split(":",$dt);
	my $p = $params->{'data_files_dir'}."/$inputSequenceType/$inputType";
	$p=~s/\/\//\//g;
	$params->{'logger'}->LogError("startStandAlone::runAnnotator() - Directory $p does not exist.") unless (-d($p));
	$paths->{$inputSequenceType}->{$inputType} = $p;
    } 

    foreach my $inputSequenceType (keys(%$paths)){
	foreach my $inputType (keys(%{$paths->{$inputSequenceType}})){
	    my $path = $paths->{$inputSequenceType}->{$inputType};
	    my $configs = $annotationTools->getElementsByAttributeHash({input_type=>$inputType,input_sequence_type=>$inputSequenceType});	    
	    my $aConfigs_tmp = $params->{'tool_register'}->getCollection()->getElementsByAttributeHash({type=>'prediction',sub_type=>'attribute',input_sequence_type=>'cds'});
	    my $aConfigs = GePan::Collection::ToolConfig->new();
	    while(my $config = $aConfigs_tmp->getNextElement()){
		$aConfigs->addElement($config) if $params->{'tools'}->{$config->getID()};
	    }
    
	    # get sequence file in directory
	    opendir(IN,$path) or $params->{'logger'}->LogError("startStandAlone::runAnnotator() - Failed to open directory $path");
	    my @files = grep {-f "$path/$_"}readdir(IN);
	    close(IN);
	    $params->{'logger'}->LogError("startStandAlone::runAnnotator() - More than one sequence fasta file found in directory $path") if (scalar(@files)>1);
	    $params->{'logger'}->LogError("startStandAlone::runAnnotator() - No sequence fasta file found in directory $path") unless scalar(@files);

	    # read in the fasta file
	    my $parser = GePan::Parser::Input::Fasta->new();
	    $parser->setParams({file=>"$path/".$files[0],
				type=>$inputSequenceType,
			        logger=>$params->{'logger'}});
	    $parser->parseFile();

	    # createAnnotator object
	    my $annotator = GePan::Annotator->new();
	    $annotator->setParams({logger=>$params->{'logger'},
				   work_dir=>$params->{'work_dir'},
				   annotation_tools=>$configs,
				   attribute_tools=>$aConfigs,
				   sequences=>$seqs,
				   type=>$inputSequenceType,
				   database_collection=>$params->{'db_register'}->getCollection()});
	    $annotator->annotate();
	}
    }

    return $seqs;
}



=head2 B<runPipeline()>

Configures and runs all tools defined in $params->{'pipeline'}.

=cut

sub runPipeline{
    my $params = shift;

    my $pipeline = $params->{'pipeline'};

    # each root contains an array-ref of tool-config objects
    foreach my $root (@$pipeline){
	# configure and run tools
	foreach my $config (@$root){
	    $params->{'logger'}->LogStatus("Preparing tool ".$config->getID());
	    my ($toolClass,$toolParams) = $params->{'configurator'}->prepareTool($config);
	    my $outputFile = $toolParams->{'output_file'};
	    $toolParams->{'output_file'} = "";

	    # initialize and execute tool
	    $params->{'logger'}->LogStatus("Initializing tool ".$config->getID());
	    eval{
		_runTool($params,$toolClass,$toolParams,$config,$outputFile);
	    };
	    $params->{'logger'}->LogError("startStandAlong::runPipeline() - $@") if $@;
	}
	# check if any exporters have to be run for any tool
	foreach my $config (@$root){
	    next unless (!(ref($config->getOutputFormat())));
	    $params->{'logger'}->LogWarning("startStandAlone::runPipeline() - Output format not fasta. Skipping export of tool ".$config->getID()) unless (lc($config->getOutputFormat()) eq 'fasta');
	    $params->{'logger'}->LogStatus("Preparing exporter for tool ".$config->getID());
	    my ($parserClass,$parserParams,$exporterOutDir) = $params->{'configurator'}->prepareToolExporter($config,$params);
	    
	    # exporting fasta sequences of tool
	    $params->{'logger'}->LogStatus("Initializing fasta exporter for result files of tool ".$config->getID());
	    eval{
		_runToolExporter($parserClass,$parserParams,$exporterOutDir,$config->getOutputType());
	    };
	    $params->{'logger'}->LogError("startStandAlong::runPipeline() - $@") if $@;
	}
    }



}



=head2 B<_runToolExporter(class,ref,types)>

Reads in the result file of a particular tool and exports them in fasta format.

=cut

sub _runToolExporter{
    my ($class,$params,$outDir,$types) = @_;
   
    my $module = $class;
    $module=~s/::/\//g;
    require "$module.pm"; 
    my $parser = $class->new();
    $parser->setParams($params);
    $parser->parseFile();

    my $exporter = GePan::Exporter::Fasta->new();
    $exporter->setParams({collection=>$parser->getCollection(),
			  file=>"exporter.fas",
			  logger=>$parser->getLogger(),
			  output_directory=>$outDir,
			  output_types=>$types});
    $exporter->export();
}



=head2 B<_runTool(ref,class-string,ref)>

Executes all tools. Annotation tools are also run on all databases specified.

=cut

sub _runTool{
    my ($params,$class,$toolParams,$config,$outputFile) = @_;

    my $module = $class;
    $module=~s/::/\//g;
    require "$module.pm";
    my $tool = $class->new();

    # build a list of all supported databases of annotation tools
    if(($config->getType() eq 'annotation')){
	my $tool_db_format = $config->getDBFormat();
	my $tool_db_type = $config->getDBType();
	my $dbs = [];

	my $collection = $params->{'db_register'}->getCollection()->getElementsByAttributeHash({database_format=>$tool_db_format,sequence_type=>$tool_db_type});
	$params->{'logger'}->LogError("startStandAlone::_runTool() - No databases with matching type or format found.") unless (($collection)&&($collection->getSize()));
	while(my $db_config = $collection->getNextElement()){
	    my $db_taxon = $db_config->getDatabaseTaxon();
	    if(($params->{'taxa'})&&($db_taxon ne 'all')){
		push @$dbs,$db_config if ($params->{'taxa'}=~m/$db_taxon/i);
	    }
	    else{
		push @$dbs,$db_config;
	    }
	}
	
	foreach my $db (@$dbs){
	    $toolParams->{'database'} = $db;
	    $toolParams->{'output_file'} = $db->getID().".".$outputFile;	  
	    chdir($toolParams->{'output_dir'}); 
	    $tool->setParams($toolParams);
	    $tool->execute();
	}
    }
    else{
	$toolParams->{'output_file'} = $outputFile;
	chdir($toolParams->{'output_dir'}); 
	$tool->setParams($toolParams);
	$tool->execute();
    }
}

=head2 B_runToolParser()>

Runs parser for prediction tools and adds all result sequences to given collection.

=cut

sub _runToolParser{
    my ($class,$params,$collection) = @_;

    my $module = $class;
    $module=~s/::/\//g;
    require "$module.pm";
    my $parser = $class->new();
    $parser->setParams($params);
    $parser->parseFile();

    while(my $seq = $parser->getCollection->getNextElement()){
	$collection->addElement($seq);
    }
}


=head2 B<_registerDBs()>

Reads in all DatabaseDefinition files and creates DatabaseRegister Objects.

=cut

sub _registerDBs{
    my $params= shift;

    # register all defined databases
    my $databaseRegister = GePan::DatabaseRegister->new();
    my $p = {'logger'=>$params->{'logger'}};
    if($params->{'db_def_dir'}){
        $p->{'config_dir'} = $params->{'db_def_dir'};
    }
    else{
	$p->{'config_dir'} = "../DatabaseDefinitions";
    }
    $databaseRegister->setParams($p);
    $databaseRegister->register();
    return $databaseRegister;
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
    if($params->{'tool_def_dir'}){
	$p->{'config_dir'} = $params->{'tool_def_dir'};
    }
    elsif(-d "../GePan/ToolDefinitions"){
	$p->{'config_dir'} = "../ToolDefinitions";
    }
    else{
	$p->{'config_dir'} = "../ToolDefinitions";
    }
    $toolRegister->setParams($p);
    $toolRegister->register();
    return $toolRegister;
}
    


=head2 B<_preprareGePan(ref)>

Prepares the initial run for the tool pipeline as well as creating the pipeline.

=cut

sub _prepareGePan{
    my $params = shift;

    # create directory for GePan run
    my $time = time;
    my $work_dir=$params->{'work_dir'}."/$time";
    $work_dir=~s/\/\//\//g;
    $params->{'logger'}->LogStatus("Creating working directory $work_dir.");
    $params->{'work_dir'} = $work_dir;
    my $e = system("mkdir ".$params->{'work_dir'});
    $params->{'logger'}->LogError("startStandAlong::_main() - Failed to create working directory ".$params->{'work_dir'}.".") if ($e);


    # create directory for result files
    my $resultPath = $params->{'work_dir'}."/results";
    $resultPath=~s/\/\//\//g;
    $params->{'logger'}->LogStatus("Creating directory for result files: $resultPath");    
    $e = system("mkdir $resultPath");
    $params->{'logger'}->LogError("startStandAlone::_prepareGePan() - Failed to create directory for result files \'$resultPath\'") if $e;
    $params->{'result_files_dir'} = $resultPath;

    # create directories for input files and data types
    my $dataPath = $params->{'work_dir'}."/data";
    $dataPath=~s/\/\//\//g;
    $params->{'logger'}->LogStatus("Creating directory for data-types: $dataPath");
    $e = system("mkdir $dataPath");
    $params->{'logger'}->LogError("startStandAlone::_prepareGePan() - Failed to create directory for data types.") if $e;
    $params->{'data_files_dir'} = $dataPath;

    # create directory for tool result files
    my $toolPath = $params->{'work_dir'}."/tools";
    $toolPath =~s/\/\//\//g;
    $params->{'logger'}->LogStatus("Creating directory for tool result files: $toolPath");
    $e = system("mkdir $toolPath");
    $params->{'logger'}->LogError("startStandAlone::_prepareGePan() - Failed to create directory for tool output files.") if $e;
    $params->{'tool_files_dir'} = $toolPath;


    # create directory for input files depending on the data type
    my $inputPath = "$dataPath/".(lc($params->{'sequence_input_type'}));
    $inputPath=~s/\/\//\//g;
    $params->{'logger'}->LogStatus("Creating directory for input file data type: $inputPath");
    $e = system("mkdir $inputPath");
    $params->{'logger'}->LogError("startStandAlone::_prepareGePan() - Failed to create directory for input data types.") if $e;
    $inputPath .="/".(lc($params->{'sequence_type'}));
    $inputPath=~s/\/\//\//g;
    $params->{'logger'}->LogStatus("Creating directory for input file sequence data type: $inputPath");
    $e = system("mkdir $inputPath");
    $params->{'logger'}->LogError("startStandAlone::_prepareGePan() - Failed to create directory for input data types.") if $e;

    # copy input files to data-type directory
    my $inputFile = $params->{'fasta'};
    $e = system("cp $inputFile $inputPath");
    $params->{'logger'}->LogStatus("Copy input files.");
    $params->{'logger'}->LogError("startStandAlone::_prepareGePan() - Failed to copy input files to data-type directory.") if $e;

    # create pipeline
    $params->{'logger'}->LogStatus("Creating tool pipeline");
    my $pipeline = GePan::PipelineCreator->new();
    my @ut = (keys(%{$params->{'tools'}}));
    $pipeline->setParams({'registered_tools'=>($params->{'tool_register'}->getCollection()),
			  'user_tools'=>\@ut,
			  'logger'=>$params->{'logger'}
			 });
    $pipeline->createPipeline();
    $params->{'pipeline'} = $pipeline->getPipeline();

    # check if initial input files are of correct type and sequence type for all initial tools
    foreach my $config (@{$params->{'pipeline'}->[0]}){
	if(lc($config->getInputSequenceType()) ne lc($params->{'sequence_input_type'})){
	    $params->{'logger'}->LogError("startStandAlone::_prepareGePan() - Inappropriate given input sequence type: given \'".$params->{'sequence_input_type'}."\' but \'".$config->getInputSequenceType()."\' is required for tool ".$config->getID()); 
	}
	elsif(lc($config->getInputType()) ne lc($params->{'sequence_type'})){
	    $params->{'logger'}->LogError("startStandAlone::_prepareGePan() - Inappropriate given input sequence type \'".$params->{'sequence_type'}."\' for tool ".$config->getID()); 
	}
    }

}



=head2 B<_rerunAnnotation($params)>

Re-runs the annotation process on an old GePan-run directory.

=cut

sub _rerunAnnotation{
    my $params = shift;

    printLOG($params->{'log'},"## Starting annotation process ...");

    # create Annotator object
    my $annotator = GePan::Annotator->new($params->{'work_dir'});
    $annotator->setOldWorkDir($params->{'old_work_dir'});
    $annotator->setExporterType($params->{'exporterType'});

    # start annotation process
    $annotator->annotate();

    # export results
    _exportResults($params);

    # estimate run-time
    $params->{'running_done'}= time;
    printRunTime($params);
    exit;
}



=head2 B<exportResults()>

Exports annotated sequences to working directory.

=cut

sub exportResults{
    my $params = shift;

    my $exporter;
    my $exporterParams = {output_directory=>$params->{'work_dir'},
                  collection=>$params->{'sequences'}};

    $exporter = GePan::Exporter::XML::SimpleAnnotation->new();
    $exporterParams->{'file'} = "gepanResultSimple.xml";

}




=head2 B<printRunTime()>

Prints runtime of whole pipeline.

=cut

sub printRunTime{
    my $params = shift;
    my $time = ($params->{'running_done'})-($params->{'running_since'});
    my $minutes = $time/60;
    $params->{'logger'}->LogStatus("GePan run completed. Runtime apprx $minutes minutes.");
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
        $logger->LogError("startStandAlone::_createToolHash - Wrong number of elements in split of tool string.") unless scalar(@toolSplit)<=2;
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


=head2 B<_createSequences()>

Reads in all input and prediction files and creates a GePan::Collection::Sequence object of all sequences.

=cut

sub _createSequences{
    my $params = shift;

    # get all prediction tools (not sub_type attribute)
    my $allPredictionTools = $params->{'tool_register'}->getCollection()->getElementsByAttributeHash({type=>'prediction'});
    my $predictionTools = GePan::Collection::ToolConfig->new();
    while(my $config = $allPredictionTools->getNextElement()){
	next if ($config->{'sub_type'} eq 'attribute');
	next unless ($config->getOutputType());
	next unless $params->{'tools'}->{$config->getID()};
	$predictionTools->addElement($config);
    }   

    # create list for all predicted sequences
    my $seqs = GePan::Collection::Sequence->new();
    $seqs->setLogger($params->{'logger'});

    while( my $config = $predictionTools->getNextElement()){
	# get params for parser
	my ($parserClass,$parserParams) = $params->{'configurator'}->prepareToolParser($config);
	eval{_runToolParser($parserClass,$parserParams,$seqs)};
	$params->{'logger'}->LogError("startStandAlone::_createSequences() - $@") if ($@);
    }
    return $seqs;
}


=head2 B<_checkScriptParameter($params)>

Performes several checks on the given command-line parameters.

=cut

sub _checkScriptParameter{
    my $params = shift;

    if($params->{'old_work_dir'}){
        $params->{'logger'}->LogError("Given old working directory does not exist.") unless (-d $params->{'old_work_dir'});
    }
    elsif(!$params->{'exporter_type'}||!$params->{'work_dir'}||!$params->{'fasta'}||!$params->{'sequence_input_type'}||!$params->{'sequence_type'}){
        _usage();
    }
    elsif(($params->{'sequence_type'} ne 'nucleotide')&&($params->{'sequence_type'} ne 'protein')){
	_usage();
    }
}











sub _usage{
    print STDOUT "\n ---- startStandAlone ----\n";
    print STDOUT "Starts the GePan pipeline stand-alone, i.e. on a usual computer without queueing system.\n";
    print STDOUT "Parameter:\n";
    print STDOUT "w : working directory.\n";
    print STDOUT "f : fasta file of sequences or directory of fasta files\n";
    print STDOUT "p : String of tool names. String is of form \"tool_name:parameter_name1=parameter_value,paramter_name2=parameter_value;...\".\n";
    print STDOUT "    For parameter without value the value is to be left empty (for more information see gepan documentation).\n";
    print STDOUT "T : type of input sequence, either nucleotide or protein\n";
    print STDOUT "S : type of input sequences, e.g. contig or CDS.\n";
    print STDOUT "t : string of taxa seperated by \',\'. If set just databases of given taxa are searched.\n";
    print STDOUT "c : number of CPUs used (optional, default = 1)\n";
    print STDOUT "o : Output generator of choice\n    1 = Complete (human readable) output tab separated (GePan::Exporter::CompleteTabSeparated)\n";
    print STDOUT "    1a = strict embl export using just qualifiers defined for embl format\n";
    print STDOUT "    1b = embl export with qualifiers unique for each tool run (not conform with defined embl-format scheme)\n";
    print STDOUT "    2 = Functional (human readable) output tab separated (GePan::Exporter::FunctionalTabSeparated)\n";
    print STDOUT "    3 = Simple output in xml-format (GePan::Exporter::XML::SimpleAnnotation)\n";
    print STDOUT "    4 = Complete project output in xml-format (GePan::Exporter::XML::Project)\n";
    print STDOUT "r : Old working directory. Parameter used for re-running the annotation on already run tools.\n";
    print STDOUT "d : Directory of tool definition files (optional, default: ../ToolDefinitions)\n";
    print STDOUT "b : Directory of database definition files (optional, default: ../DatabaseDefinitions)\n";
    print STDOUT "\n";
    print STDOUT "\n";
    exit;
}

