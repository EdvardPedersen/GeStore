#!/usr/bin/perl

use strict;
use lib "/home/emr023";
use Data::Dumper;
use Getopt::Std;
use GePan::Collection::Sequence;
use GePan::ToolRegister;
use GePan::DatabaseRegister;
use GePan::PipelineCreator;
use GePan::Config qw(PERL_PATH GEPAN_PATH NODE_LOCAL_PATH GESTORE_PATH GESTORE_CONFIG);

=head1 NAME

startGePan.pl

=head1 DESCRIPTION

Script to start the GePan pipeline on a computer cluster. Supported queuing systems are Sun Grid Engine (SGE) and Torque PBS.


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

o:  Output generator of choice
    
    1 = Complete (human readable) output tab separated (GePan::Exporter::CompleteTabSeparated)

    2 = Functional (human readable) output tab separated (GePan::Exporter::FunctionalTabSeparated)

    3 = Simple output in xml-format (GePan::Exporter::XML::SimpleAnnotation)

    4 = Complete project output in xml-format (GePan::Exporter::XML::Project)

r: Just annotator is run on old working directory

q: Parameter string for queuing system starting either with 'sge' or 'pbs'. Parameter are seperated by ",". 

    Example for running GePan SGE: 'sge:walltime=00:60:00,cpu....'

    Supported parameter are:

    - walltime: time the pipeline is supposed to run (hours:minutes:seconds)

    - cpu: number of cpus requested for the pipeline to run on

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
    getopts("b:d:w:f:p:t:c:T:S:q:r:o:s:PRG:",\%opts);

    my $params = {work_dir => $opts{'w'},
                  fasta => $opts{'f'},
                  tool_string => $opts{'p'},
                  taxa => $opts{'t'}?$opts{'t'}:0,
                  queueing  => $opts{'q'},
                  sequence_input_type => $opts{'S'},
                  exporter_type => $opts{'o'},
                  old_work_dir => $opts{'r'},
                  sequence_type => $opts{'T'},
                  tool_def_dir=>$opts{'d'},
                  db_def_dir=>$opts{'b'},
		  sorting=>$opts{'s'},
                  performance=>$opts{'P'},
                  submit_shells=>$opts{'R'},
                  gestore=>$opts{'G'}
    };
    
    if($params->{'gestore'}) {
        print "gestore enabled";
    } else {
        print "gestore disabled";
    }

    # check given parameter
    _checkScriptParameter($params);

    # create all needed directories
    $params->{'script_id'} = _createDirectoryStructure($params);

    # register all known tools of pipeline
    $params->{'tool_register'} = _registerTools($params);

    # register all known databases
    $params->{'db_register'} = _registerDBs($params);

    # check chosen tools and databases
    _checkToolDBs($params);

    # create pipeline
    _createPipeline($params);

    # create sub-directories for tools and input files
    _createSubDirectories($params);

    # write file of start parameter
    _writeStartParameter($params);

    # write shell scripts for tools
    _createShells($params);

    # submit shell scripts
    _submitShells($params);
}

sub _timeStart{
    my ($params, $name, $fileHandle) = @_;
    if($params->{'performance'})
    {
        print $fileHandle "# Timer information stored in timeFile\n";
        print $fileHandle "timeFile=\"".$params->{'work_dir'}."/logs/".$name.".time.log\"\n";
        print $fileHandle "(echo -n \"".$name." start: \"; date \+\%s) | xargs >> \$timeFile\n";
    }
}

sub _timeStop{
    my ($params, $name, $fileHandle) = @_;
    if($params->{'performance'})
    {
        print $fileHandle "# Stop the timer\n";
        print $fileHandle "(echo -n \"".$name." stop: \"; date \+\%s) | xargs >> \$timeFile\n";
    }
}

sub _printGeStoreCall{
    my ($file, $args) = @_;
    
    #path, task, run, type, filename
    
    my $hadoopLine = "hadoop jar ".GESTORE_PATH." org.diffdb.move ";
    my $fileLine = "-D file=".$args->{'filename'}." ";
    my $runLine = "-D run=".$args->{'run'}." ";
    my $typeLine = "-D type=".$args->{'type'}." ";
    my $confLine = "-conf=".GESTORE_CONFIG."\n";
    my $pathLine = "";
    my $taskLine = "";
    my $formatLine = "";
    
    
    if($args->{'path'}) {
        $pathLine = " -D path=".$args->{'path'}." ";
    }
    
    if($args->{'task'}) {
        $taskLine = ' -D task='.$args->{'task'}." ";
    }
    
    if($args->{'format'}) {
        $formatLine = " -D format=".$args->{'format'}." ";
    }
    print $file $hadoopLine.$fileLine.$runLine.$pathLine.$taskLine.$typeLine.$formatLine.$confLine;
}

sub _submitShells{
    my $params = shift;
   
    my $statements = [];
    my $pipeline = $params->{'pipeline'};
    my $dep;
    
    foreach my $level (@$pipeline){
	foreach my $config (@$level){
	    my $statement = "qsub";
	    if((($config->getType() eq 'annotation')||(($config->getType() eq 'prediction')&&($config->getSubType() eq 'attribute')))&&($config->getID() ne 'megan')){
		$statement .= " -t 1-".$params->{'queueing'};
	    }	
	    if($dep){
		$statement .= " -hold_jid $dep"."_".$params->{'script_id'};
	    }
	    $statement .= " ".$params->{'shell_files_dir'}."/".$config->getID().".sh";
	    push @$statements,$statement;

	    if(-f ($params->{'shell_files_dir'}."/".$config->getID().".exporter.sh")){
		my $exporterStatement = "qsub -hold_jid ".$config->getID()."_".$params->{'script_id'}." ".$params->{'shell_files_dir'}."/".$config->getID().".exporter.sh";
		my $schedulerStatement = "qsub -hold_jid ".$config->getID().".exporter_".$params->{'script_id'}." ".$params->{'shell_files_dir'}."/fileScheduler.sh";
		push @$statements,$exporterStatement;
		push @$statements,$schedulerStatement;
		$dep = "fileScheduler";
	    }
	    else{
		$dep = $config->getID();
	    }
	}
    }

    my $annotatorStatement = "qsub -hold_jid $dep"."_".$params->{'script_id'}." -t 1-".$params->{'queueing'}." ".$params->{'shell_files_dir'}."/annotator.sh";
    push @$statements,$annotatorStatement;

    my $exporterStatement = "qsub -hold_jid annotator_".$params->{'script_id'}." ".$params->{'shell_files_dir'}."/exporter.sh";
    push @$statements, $exporterStatement;

    warn $_ for @$statements;

    if($params->{'submit_shells'}){
        open(FINALSHELL, ">".$params->{'shell_files_dir'}."/submit_jobs.sh");
        print FINALSHELL '#!/bin/sh'."\n\n";
        print FINALSHELL "# This shell script submits all the jobs for GePan\n";
        foreach my $command (@$statements){
            print FINALSHELL $command . "\n";
        }
        close FINALSHELL;
    } else {
        system("$_") for @$statements;
    }
}


=head2 B<_createShells(ref)>

Creates shell script for all tools to be submitted to the cluster.

=cut


sub _createShells{

    my $params = shift;

    my $pipeline = $params->{'pipeline'};

    # check if predictions are performed in the first tool step
    my $preds = $pipeline->[0];
    $params->{'logger'}->LogError("startGePan::_createShells() - No prediction tools defined in pipeline.") unless ($preds->[0]->getType() eq 'prediction');

    # create prediction/first level shell scripts
    foreach my $config (@$preds){
	my $file = $params->{'shell_files_dir'}."/".$config->getID().".sh";
	_printSingleShellHeader($params,$config,$file); 
	_printSingleShellCall($params,$config,$file);
    
	# check if any exporters have to be run for any tool
	# and write exporter shell-script for prediction tools	    
        next unless (!(ref($config->getOutputFormat())));
        my ($parserClass,$parserParams,$exporterOutDir) = _prepareToolExporter($params,$config,$params);

	$parserParams->{'script_id'} = $params->{'script_id'};

	my $exporterScript = $params->{'shell_files_dir'}."/".$config->getID().".exporter.sh";
	_printSingleShellHeader($parserParams,$config,$exporterScript,1,$params->{'log_files_dir'});
	_printExporterShellCall($exporterScript,$params,$config,$parserParams,$parserClass,$exporterOutDir);
    }

    # create filescheduler script
    my $schedulerFile = $params->{'shell_files_dir'}."/fileScheduler.sh";
    _printSchedulerShell($params,$schedulerFile);


    # create annotation tool Jobarrays
    for(my $i = 1;$i<=scalar(@$pipeline);$i++){
	my $level = $pipeline->[$i];
	foreach my $config (@$level){
	    my $shell = $params->{'shell_files_dir'}."/".$config->getID().".sh";
	    _printArrayShellHeader($params,$config,$shell); 
	    _printArrayShell($params,$config,$shell);
	}
    }


    # annotate results
    my $aShell = $params->{'shell_files_dir'}."/annotator.sh";
    _printAnnotatorShell($params,$aShell);

    # export results
    my $eShell = $params->{'shell_files_dir'}."/exporter.sh";
    _printExporterShell($params,$eShell);

}



=head2 B<_printArrayShell()>

Prints the tool call etc to the apropriate shell script.

=cut

sub _printArrayShell{
    my ($params,$config,$shell) = @_;
    my $queueing = $params->{'queueing'};

    open(IN,">>$shell") or $params->{'logger'}->LogError("GePan::_startGePan::_printArrayShell() - Failed to open shell-script $shell for writing.");

    
    # create directory of input files and copy them over in case it doesnt exist.
    my $ist = $config->getInputSequenceType();
    my $st = $config->getInputType();
    my $inputPath = $params->{'data_files_dir'}."/$ist/$st/tmp";
    $inputPath=~s/\/\//\//g;
    
    _timeStart($params, $config->getID().'_${SGE_TASK_ID}', \*IN);

    # create directories on node
    print IN "\n# Create working directory for job\n";
    print IN "if [ ! -d ".NODE_LOCAL_PATH."/gepan ]\nthen\n\tmkdir -m 777 ".NODE_LOCAL_PATH."/gepan\nfi\n";
    print IN "cd ".NODE_LOCAL_PATH."/gepan\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}'."\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/input'."\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/output'."\n";

    # create input file path and copy it to node
    if($params->{'gestore'})
    {
        _printGeStoreCall( *IN, {'filename'=>$config->getInputType()."_scheduler.tar", 
                                 'type'=>"r2l", 
                                 'run'=>$params->{'script_id'} } );
        #print IN 'hadoop jar '.GESTORE_PATH.' org.diffdb.move -D file='.$params->{'script_id'}.$config->getInputType()."_scheduler.tar -D run=".$params->{'script_id'}." -D type=r2l -conf=".GESTORE_CONFIG."\n";
        print IN "tar -xf ".$config->getInputType()."_scheduler.tar --directory=".NODE_LOCAL_PATH."/gepan/".'${JOB_ID}'.'_'.'${SGE_TASK_ID}/input/ ./exporter.fas.$SGE_TASK_ID'."\n";
    } else {
        my $input = $params->{'data_files_dir'}."/".$config->getInputSequenceType()."/".$config->getInputType().'/tmp/exporter.fas.$SGE_TASK_ID';
        print IN "\n# Copy input files to node\n";
        print IN "cp $input ".NODE_LOCAL_PATH."/gepan/".'${JOB_ID}'.'_'.'${SGE_TASK_ID}/input'."\n";
    }

    # create tool execute statement
    my ($toolClass,$toolParams) = _prepareTool($params,$config);

    $toolParams->{'output_dir'} = NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/output';
    $toolParams->{'input_file'} = NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/input/exporter.fas.$SGE_TASK_ID';

    print IN "\n# execute statement(s) of tool\n";

    # get databases annotation tools run on
    if($config->{'db_format'}){
	# get databases of defined taxa or use all
	my $d = $params->{'db_register'}->getCollection()->getElementsByAttributeHash({sequence_type=>$config->getInputType(),database_format=>$config->getDBFormat()});
	my $dbs;
	if($params->{'taxa'}){
	    $dbs = GePan::Collection::ToolConfig->new();
	    my @taxa = split(",",$params->{'taxa'});
	    while(my $db_config = $d->getNextElement()){
		foreach(@taxa){
		    if($db_config->getDatabaseTaxon()=~m/$_/){
			$dbs->addElement($db_config);
		    }
		}
	    }
	}
	else{
	    $dbs = $params->{'db_register'}->getCollection()->getElementsByAttributeHash({sequence_type=>$config->getInputType(),database_format=>$config->getDBFormat()});
	}

	while(my $db_config = $dbs->getNextElement()){  
            $toolParams->{'database'} = $db_config;
            $toolParams->{'output_file'} = 'exporter.fas.'.$db_config->getID().'.'.$config->getID().'.out.$SGE_TASK_ID';
            $toolParams->{'run'} = $params->{'script_id'};
            $toolParams->{'regex'} = $params->{'gestore'};
 #           if($params->{'gestore'}) {
 #               my @db_path = split('/', $toolParams->{'database'}->getPath());
 #               my $db_name = $db_path[-1];
#                
#                my $db_statement = 'hadoop jar '.GESTORE_PATH.' org.diffdb.move -D file='.$params->{'script_id'}.$db_name.' -D run='.$toolParams->{'run'}.' -D type=r2l -D regex='.$toolParams->{'regex'}."-conf=".GESTORE_CONFIG."\n";
#                print GE $db_statement."\n";
#            }
            eval{
                _initializeTool($toolClass,$toolParams,*IN,$queueing);
            };
            $params->{'logger'}->LogError("startGePan::_printSingleShellCall() - $@") if ($@);
            print IN "\n";
        }
    }
    else{
	$toolParams->{'output_file'} = 'exporter.fas.'.$config->getID().'.out.$SGE_TASK_ID';
	eval{
            _initializeTool($toolClass,$toolParams,*IN,$queueing);
        };
        $params->{'logger'}->LogError("startGePan::_printSingleShellCall() - $@") if ($@);
    }

    print "Input format GS: ".$config->getGsInputFormat()."\n";
    print "Output format GS: ".$config->getGsOutputFormat()."\n";

    # copy result files to working directory in home
    print IN "\n# Copy result files to home directory\n";
    if($params->{'gestore'})
    {
        if($config->{'id'} eq 'priam'){
            print IN "tar -cf results.tar --directory=".NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/output/RESULTS/ ./paj_exporter.fas.priam.out.${SGE_TASK_ID}'."_seqsECs.txt\n";
            _printGeStoreCall(*IN, {'filename'=>$params->{'script_id'}.'${SGE_TASK_ID}_'.$config->getID().'_out'.".tar",
                                    'run'=>$params->{'script_id'},
                                    'path'=>"results.tar",
                                    'type'=>"l2r" } );
            #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}.'${SGE_TASK_ID}_'.$config->getID().'_out'.".tar -D run=".$params->{'script_id'}." -D path=results.tar -D type=l2r -conf=".GESTORE_CONFIG."\n";
        }else{
            print IN "tar -cf results.tar --directory=".NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/output/ .'."\n";
            _printGeStoreCall(*IN, {'filename'=>$params->{'script_id'}.'${SGE_TASK_ID}_'.$config->getID().'_out'.".tar",
                                    'run'=>$params->{'script_id'},
                                    'path'=>"results.tar",
                                    'type'=>"l2r" } );
            #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}.'${SGE_TASK_ID}_'.$config->getID().'_out'.".tar -D run=".$params->{'script_id'}." -D path=results.tar -D type=l2r -conf=".GESTORE_CONFIG."\n";
        }
    } else {
        # bad bad bad bad bad bad bad bad.... thanks NP!!!!
        if($config->{'id'} eq 'priam'){
            print IN "mv ".NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/output/RESULTS/paj_exporter.fas.priam.out.'.'${SGE_TASK_ID}'."_seqsECs.txt ".$params->{'tool_files_dir'}."/".$config->getID()."/exporter.fas.priam.out.".'${SGE_TASK_ID}'."\n";
        }else{
            print IN 'mv '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/output/* '.$params->{'tool_files_dir'}."/".$config->getID()."\n";
        }
    }


    # cleanup
    print IN "\n# Cleanup\n";
    if($config->{'id'} eq 'priam'){
	print IN 'rm -Rf '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/output/*'."\n";
    }
    print IN 'rmdir '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/output'."\n";
    print IN 'rm '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/input/*'."\n";
    print IN 'rmdir '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/input'."\n";
    print IN 'rmdir '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}'."\n";

    _timeStop($params, $config->getID().'_${SGE_TASK_ID}', \*IN);
}


=head2 B<_printExporterShell()>

Prints shell file for exporting sequences.

=cut

sub _printExporterShell{
    my ($params,$shell) = @_;

    open(IN,">$shell") or $params->{'logger'}->LogError("GePan::_startGePan::_printAnnotatorHeader() - Failed to open shell-script $shell for writing.");
    print IN '#!/bin/sh'."\n\n";
    print IN '# This script was automatically created by the Gene prediction and annotation pipeline GePan developed by Tim Kahlke.'."\n";
    print IN '# Don\'t modify this script except you know what you are doing.'."\n\n";

    # print Name of job
    print IN '#$ -N exporter_'.$params->{'script_id'}."\n";
    print IN '#$ -S /bin/bash'."\n";

    # join STDOUT and STDERR for output
    print IN '#$ -j y'."\n";

    # log file
    print IN '#$ -o '.$params->{'log_files_dir'}.'/exporter.log'."\n";
    
    _timeStart($params, "exporter", \*IN);
   
    # create directory for job
    print IN "\n# create job working dir\n";
    print IN 'if [ ! -d '.NODE_LOCAL_PATH.'/gepan/$JOB_ID ]'."\nthen\n\tmkdir ".NODE_LOCAL_PATH.'/gepan/$JOB_ID'."\nfi\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/input'."\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output'."\n";

    # copy xml files to node
    print IN "\n# Copy xml files to node\n";
    print IN "mv ".$params->{'result_files_dir'}."/*.dump ".NODE_LOCAL_PATH.'/gepan/$JOB_ID/input'."\n";

    # copy parameter.xml to node
    print IN "\n# Copy parameter.xml to node\n";
    print IN "cp ".$params->{'work_dir'}."/parameter.xml ".NODE_LOCAL_PATH.'/gepan/$JOB_ID/'."\n";

    # copy parent fasta file over
    print IN "\n# Copy parent fasta file over\n";
    print IN "cp ".$params->{'data_files_dir'}."/".$params->{'sequence_input_type'}."/".$params->{'sequence_type'}."/input.fas ".NODE_LOCAL_PATH.'/gepan/$JOB_ID/input'."\n";

    # print shell script call
    print IN "\n# Call exporter script\n";
    print IN PERL_PATH.' -I '.GEPAN_PATH.' '.GEPAN_PATH.'/GePan/scripts/runExporter.pl -p '.NODE_LOCAL_PATH.'/gepan/$JOB_ID'."\n";     

    # copy result file over 
    print IN 'cp '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output/* '.$params->{'work_dir'}."/results/\n";

    # remove directory
    print IN "\n# Cleanup\n";
    print IN 'rm -Rf '.NODE_LOCAL_PATH.'/gepan/$JOB_ID'."\n";
    
    _timeStop($params, "exporter", \*IN);
}


=head2 B<_printAnnotatorShell()>

Prints header of shell script for annotation and export of results.

=cut

sub _printAnnotatorShell{
    my ($params,$shell) = @_;

    open(IN,">$shell") or $params->{'logger'}->LogError("GePan::_startGePan::_printAnnotatorHeader() - Failed to open shell-script $shell for writing.");
    print IN '#!/bin/sh'."\n\n";
    print IN '# This script was automatically created by the Gene prediction and annotation pipeline GePan developed by Tim Kahlke.'."\n";
    print IN '# Don\'t modify this script except you know what you are doing.'."\n\n";

    # print Name of job
    print IN '#$ -N annotator_'.$params->{'script_id'}."\n";
    print IN '#$ -S /bin/bash'."\n";

    # join STDOUT and STDERR for output
    print IN '#$ -j y'."\n";

    # log file
    print IN '#$ -o '.$params->{'log_files_dir'}.'/annotator.log'."\n";
    
    _timeStart($params, "annotator", \*IN);

    # create directory for task
    print IN "\n# create task working dir\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}'."\n";

    # create directory structure similar to gepan working dir
    my $pipeline = $params->{'pipeline'};
   
    # copy arameter.xml to node
    print IN "\n# copy parameter.xml to node\n";
    if($params->{'gestore'})
    {
	_printGeStoreCall(*IN, {'file'=>"parameter.xml", 'run'=>$params->{'script_id'}, 'path'=>$params->{'work_dir'}."/parameter.xml", 'type'=>'l2r'});
	_printGeStoreCall(*IN, {'file'=>"parameter.xml", 'run'=>$params->{'script_id'}, 'type'=>'r2l'});
        #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."parameter.xml -D run=".$params->{'script_id'}." -D path=".$params->{'work_dir'}."/parameter.xml -D type=l2r -conf=".GESTORE_CONFIG."\n";
        #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."parameter.xml -D run=".$params->{'script_id'}." -D type=r2l -conf=".GESTORE_CONFIG."\n";
        print IN "mv parameter.xml ".NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/'."\n";
    } else {
        print IN "cp ".$params->{'work_dir'}.'/parameter.xml '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}'."\n";
    }
 
    # create data directory
    print IN "\n# create task working dir structure\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/data'."\n";

    # create tool directory
    print IN "\n# Create tool files directory\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/tools'."\n";
    
    # create results file
    print IN "\n# Create result files directory\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/results'."\n";

    # 1. create sub directories, i.e. directories for tool result files, tool input files and input files of following tools
    # 2. additionally copy files needed for each annotator task
    foreach my $level (@$pipeline){
        foreach my $config (@$level){

	    # check if there is an collection.xml file of all tools that have an output type, i.e. are not used 
	    # for annotation but as input types for annotation
	    if(($config->getOutputSequenceType())&&(!(ref($config->getOutputSequenceType())))){
		# check if there is a collection.xml file in output directory.
		# If so copy it to node 
		print IN "\n# copy collection.xml file to node if existing\n";
                
                if($params->{'gestore'})
                {
                    #UGLY HACK! Should replace entire code segment -epe
                    #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."collection.xml -D run=".$params->{'script_id'}." -D path=".$params->{'data_files_dir'}."/".$config->getOutputSequenceType()."/ -D type=r2l -conf=".GESTORE_CONFIG."\n";
                    _printGeStoreCall(*IN, {'file'=>"collection.xml", 'run'=>$params->{'script_id'}, 'type'=>'r2l'});
                    print IN "mv collection.xml ".$params->{'data_files_dir'}."/".$config->getOutputSequenceType()."/\n";
                }
                
		# if collection xml
		print IN "if [ -f ".$params->{'data_files_dir'}."/".$config->getOutputSequenceType()."/collection.xml ]\nthen\n";
		# create directory for it
		print IN "\n\tif [ ! -d ".NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/data/'.$config->getOutputSequenceType()." ]\n\tthen\n\t\tmkdir ".NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/data/'.$config->getOutputSequenceType()."\n\tfi\n";
		# and copy it over
		print IN "\tcp ".$params->{'data_files_dir'}."/".$config->getOutputSequenceType()."/collection.xml ".NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/data/'.$config->getOutputSequenceType()."\nfi\n";
		next;
	    }
            
            # create directory for tool result files   
            print IN "\n# Create tool result files directory\n";
            print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/tools/'.lc($config->getID())."\n"; 

	    # copy one fasta file over
            if($params->{'gestore'})
            {
		_printGeStoreCall(*IN, {'file'=>"nucleotide_scheduler", 'run'=>$params->{'script_id'}, 'type'=>'r2l'});
		print IN "mv nucleotide_scheduler ".NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/data/cds/ ./*.${SGE_TASK_ID}'."\n";
		_printGeStoreCall(*IN, {'file'=>$config->getID()."_out.tar", 'task'=>{'${SGE_TASK_ID}'} 'run'=>$params->{'script_id'}, 'type'=>'r2l'});
		
                #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."nucleotide_scheduler -D run=".$params->{'script_id'}." -D path=".NODE_LOCAL_PATH.'/gepan/${JOB_ID}_${SGE_TASK_ID}/data/cds/ -D type=r2l -conf='.GESTORE_CONFIG."\n";
                # print IN "tar -xf nucleotide_scheduler.tar --directory=".NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/data/cds/ ./*.${SGE_TASK_ID}'."\n";
                
                #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."".'${SGE_TASK_ID}'."_".$config->getID()."_out.tar -D run=".$params->{'script_id'}." -D type=r2l -conf=".GESTORE_CONFIG."\n";
                print IN "tar -xf ".'${SGE_TASK_ID}'."_".$config->getID()."_out.tar --directory=".NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/tools/'.lc($config->getID())."\n";
            } else {
                print IN "\n# Copy one task fasta file over\n";
                print IN "cp ".$params->{'data_files_dir'}."/cds/nucleotide/tmp/*.".'${SGE_TASK_ID} '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/data/'."cds\n";

                # copy all tool result files ending with $SGE_TASK_ID over to tools output directory
                print IN "\n# Copy tool result files ending on sge_task_id over to appropriate directory\n";
                print IN "cp ".$params->{'tool_files_dir'}."/".lc($config->getID()).'/*.${SGE_TASK_ID} '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID}/tools/'.lc($config->getID())."\n";
            }
	}
    }

    # call annotator script
    print IN "\n# Call annotator script\n";
    print IN PERL_PATH.' -I '.GEPAN_PATH.' '.GEPAN_PATH.'/GePan/scripts/runAnnotator.pl -p '.NODE_LOCAL_PATH.'/gepan/${JOB_ID}'.'_'.'${SGE_TASK_ID} -t $SGE_TASK_ID'."\n";

    # move annotator.$SGE_TASK.xml to users result directory
    print IN "\n# Copy result *.xml file over to users working dir\n";
    if($params->{'gestore'})
    {
        print IN "tar -cf results.tar --directory=".NODE_LOCAL_PATH."/gepan/".'${JOB_ID}'.'_'.'${SGE_TASK_ID}/results/ .'."\n";
        #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."annotator.".'${SGE_TASK_ID}'.".tar -D run=".$params->{'script_id'}." -D path=results.tar -D type=l2r -conf=".GESTORE_CONFIG."\n";
        _printGeStoreCall(*IN, {'file'=>"annotator.tar", 'run'=>$params->{'script_id'}, 'task'=>'${SGE_TASK_ID}', 'type'=>"l2r", 'path'=>"results.tar"});
        #BAD BAD BAD! Hack to produce results since annotator.TASK_ID.tar is hard to get in and out reliably from GeStore
        # proper solutions: synchronization? support for wildcards? in-system archive support? alternate timestamp format?
        print IN "mv ".NODE_LOCAL_PATH."/gepan/".'${JOB_ID}'.'_'.'${SGE_TASK_ID}/results/* '.$params->{'work_dir'}."/results/\n";
    } else {
        print IN "mv ".NODE_LOCAL_PATH."/gepan/".'${JOB_ID}'.'_'.'${SGE_TASK_ID}/results/* '.$params->{'work_dir'}."/results/\n";
    }

    # Cleanup
    print IN "\n# Cleanup\n";
    print IN "rm -Rf ".NODE_LOCAL_PATH."/gepan/".'${JOB_ID}'.'_'.'${SGE_TASK_ID}'."\n";
    _timeStop($params, "annotator", \*IN);
    close(IN);
}


=head2 B<_printArrayShellHeader()>

Prints shell-script header for array-jobs.

=cut

sub _printArrayShellHeader{
    my ($params,$config,$shell) = @_;

    open(IN,">$shell") or $params->{'logger'}->LogError("GePan::_startGePan::_printArrayShellHeader() - Failed to open shell-script $shell for writing.");
    print IN '#!/bin/sh'."\n\n";
    print IN '# This script was automatically created by the Gene prediction and annotation pipeline GePan developed by Tim Kahlke.'."\n";
    print IN '# Don\'t modify this script except you know what you are doing.'."\n\n";

    # print Name of job
    print IN '#$ -N '.$config->getID().'_'.$params->{'script_id'}."\n";
    print IN '#$ -S /bin/bash'."\n";

    # set log file
    print IN '#$ -o '.$params->{'log_files_dir'}.'/'.$config->getID().'.log'."\n";

    # join STDOUT and STDERR for output
    print IN '#$ -j y'."\n";

    close(IN);
}





=head2 B<_printExporterShellCall(ref,class,exportDir)>

Writes shell script for calling exportFasta.pl.

=cut

sub _printExporterShellCall{
    my ($file,$params,$config,$parserParams,$class,$exportDir) = @_;

    open(IN,">>$file") or $params->{'logger'}->LogError("startGePan::_printExporterShellCall() - Failed to re-open shell script $file for writing.");

    _timeStart($params, $config->getID() . ".exporter", \*IN);
    
    # Create working directory on node
    print IN "\n# Create working directory for job\n";
    print IN "if [ ! -d ".NODE_LOCAL_PATH."/gepan ]\nthen\n\tmkdir -m 777 ".NODE_LOCAL_PATH."/gepan\nfi\n";
    print IN "cd ".NODE_LOCAL_PATH."/gepan\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID'."\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/input'."\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output'."\n";
    if($config->getOutputType()=~m/protein/){
	print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output/protein'."\n";
    }
    if($config->getOutputType()=~m/nucleotide/){
	print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output/nucleotide'."\n";
    }

    # copy input file to node
    print IN "\n# copy input file to node\n";
    
    if($params->{'gestore'})
    {
        #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."input.fas.".$config->getID().".out -D run=".$params->{'script_id'}." -D type=r2l -conf=".GESTORE_CONFIG."\n";
        _printGeStoreCall(*IN, {'filename'=>"input.fas.".$config->getID().".out",
				  'run'=>$params->{'script_id'},
				  'type'=>'r2l'});
        #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."input.fas -D run=".$params->{'script_id'}." -D type=r2l -conf=".GESTORE_CONFIG."\n";
        _printGeStoreCall(*IN, {'filename'=>"input.fas",
				  'run'=>$params->{'script_id'},
				  'type'=>'r2l'});
        #print IN "mv ".$params->{'script_id'}."input.fas input.fas\n";
        #print IN "mv input.fas ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/input/input.fas'."\n";
        #print IN "mv ".$params->{'script_id'}."input.fas.".$config->getID().".out input.fas.".$config->getID().".out \n";
        print IN "mv input.* ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/input/'."\n";
    } else {
        my $inputFiles = $params->{'tool_files_dir'}."/".$config->getID()."/input.fas.".$config->getID().".out";
        $inputFiles=~s/\/\//\//g;
        print IN "cp $inputFiles ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/input/'."\n";
        # get parent sequence fasta file
        my $parentInputDir = $params->{'data_files_dir'}."/".lc($config->getInputSequenceType())."/".lc($config->getInputType());
        $parentInputDir=~s/\/\//\//g;
        print IN "cp $parentInputDir/input.fas ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/input/'."\n";
    }
    
 
    # create parameterString for script
    my $paramString = "";
    $parserParams->{'file'} = NODE_LOCAL_PATH."/gepan/".'$JOB_ID/input/'."input.fas.".$config->getID().".out";
    foreach(keys(%$parserParams)){
	next if (($_ eq "logger")||($_ eq "parent_sequences"));
	$paramString.="$_=".$parserParams->{$_}.";";
    }

    # write exportFasta.pl call
    print IN "\n# Call exporter script\n";
    my $statement = PERL_PATH.' -I '.GEPAN_PATH.' '.GEPAN_PATH.'/GePan/scripts/exportFasta.pl'." -p \"$paramString\" -c \"$class\" -t \"".$config->getOutputType()."\" -s ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/input/input.fas -o '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output/'."\n";
    print IN $statement;
  
    # move result files to users home and start cleanup
    print IN "\n# Copy result files to users result dir and start cleanup\n";
    my @split = split(",",$config->getOutputType());
    foreach(@split){
        if($params->{'gestore'})
        {
            #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."".$_."_exporter.fas -D run=".$params->{'script_id'}." -D path=".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/'."output/$_/exporter.fas -D type=l2r -D format=".$config->getGsExporterOutputFormat()." -conf=".GESTORE_CONFIG."\n";
            _printGeStoreCall(*IN, {'filename'=>$_."_exporter.fas",
				  'run'=>$params->{'script_id'},
				  'type'=>'l2r',
				  'path'=>NODE_LOCAL_PATH."/gepan/".'$JOB_ID/'."output/$_/exporter.fas",
				  'format'=>$config->getGsExporterOutputFormat()});
        } else {
            print IN "mv ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/'."output/$_/exporter.fas ".$params->{'data_files_dir'}."/".$config->getOutputSequenceType()."/$_/\n";
            print IN "rmdir ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/'."output/$_\n";
        }
    }  

    # move collection.xml to work_dir/cds directory
    if($params->{'gestore'})
    {
	_printGeStoreCall(*IN, {'filename'=>"collection.xml",
				  'run'=>$params->{'script_id'},
				  'path'=>NODE_LOCAL_PATH.'/gepan/$JOB_ID/output/collection.xml',
				  'type'=>'l2r'});
        #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."collection.xml -D run=".$params->{'script_id'}." -D path=".NODE_LOCAL_PATH.'/gepan/$JOB_ID/output/collection.xml'." -D type=l2r -conf=".GESTORE_CONFIG."\n";
    } else {
        print IN 'mv '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output/collection.xml '.$params->{'data_files_dir'}."/".$config->getOutputSequenceType()."\n";
    }

    # cleanup
    print IN 'rmdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output'."\n";
    print IN 'rm '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/input/*'."\n";
    print IN 'rmdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/input'."\n";
    print IN 'rmdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID'."\n";
    _timeStop($params, $config->getID() . ".exporter", \*IN);
    close(IN);
}




=head2 B<_printSchedulerShell($params)>

Print header for filescheduler to split the cds-fasta into several.

=cut

sub _printSchedulerShell{
    my ($params,$file) = @_;

    open(IN,">$file") or $params->{'logger'}->LogError("startGePan::_printSchedulerShell() - Could not open sheel script for writing tool call");
   
    print IN '#!/bin/sh'."\n\n";
    print IN '# This script was automatically created by the Gene prediction and annotation pipeline GePan developed by Tim Kahlke.'."\n";
    print IN '# Don\'t modify this script except you know what you are doing.'."\n\n";

    print IN '#$ -S /bin/bash'."\n";
    print IN '#$ -N fileScheduler_'.$params->{'script_id'}."\n";
    print IN '#$ -o '.$params->{'log_files_dir'}.'/fileScheduler.log'."\n";
    print IN '#$ -j y'."\n";
    
    _timeStart($params, "fileScheduler", \*IN);

    print IN "\n# Create working directory for job\n";
    print IN "if [ ! -d ".NODE_LOCAL_PATH."/gepan ]\nthen\n\tmkdir -m 777 ".NODE_LOCAL_PATH."/gepan\nfi\n";
    print IN "cd ".NODE_LOCAL_PATH."/gepan\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID'."\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/input'."\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output'."\n";

    # copy nucleotide fasta over to input
    if($params->{'gestore'})
    {
        #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."nucleotide_exporter.fas -D run=".$params->{'script_id'}." -D type=r2l -conf=".GESTORE_CONFIG."\n";
        _printGeStoreCall(*IN, {'filename'=>"nucleotide_exporter.fas",
				 'run'=>$params->{'script_id'},
				 'type'=>'r2l'});
        print IN "mv nucleotide_exporter.fas exporter.fas\n";
        print IN "mv exporter.fas ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/input/'."\n";
    } else {
        # copy nucleotide fasta over to input
        print IN "\ncp ".$params->{'data_files_dir'}."/cds/nucleotide/exporter.fas ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/input/exporter.fas'."\n";
    }

    # print execute statement
    my $statement = PERL_PATH.' -I '.GEPAN_PATH.' '.GEPAN_PATH.'/GePan/scripts/runScheduler.pl -i '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/input/exporter.fas -o '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output -n '.$params->{'queueing'}.' -s '.$params->{'sorting'}."\n";
    print IN $statement;

    if($params->{'gestore'})
    {
        print IN "for i in `seq 1 ".$params->{'queueing'}."`;\n";
        print IN "do\n";
        print IN "\t";
        _printGeStoreCall(*IN, {'filename'=>"nucleotide_scheduler",
                                'run'=>$params->{'script_id'},
                                'path'=>NODE_LOCAL_PATH.'/gepan/$JOB_ID/output/exporter.fas.$i',
                                'task'=>'$i',
                                'type'=>"l2r",
                                'format'=>"fasta" } );
        print IN "done\n";

        #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."protein_exporter.fas -D run=".$params->{'script_id'}." -D type=r2l -conf=".GESTORE_CONFIG."\n";
        _printGeStoreCall(*IN, {'filename'=>"protein_exporter.fas",
				  'run'=>$params->{'script_id'},
				  'type'=>"r2l"});
        print IN "mv protein_exporter.fas exporter.fas\n";
        print IN "mv exporter.fas ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/input/'."\n";
        print IN "rm ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/output/*'."\n";
    } else {
        # Copy result files over
        print IN "mv ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/output/* '.$params->{'data_files_dir'}."/cds/nucleotide/tmp/\n";

        # copy protein fasta over
        print IN "\ncp ".$params->{'data_files_dir'}."/cds/protein/exporter.fas ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/input/exporter.fas'."\n";
    }

    # print execute statement
    my $statement = PERL_PATH.' -I '.GEPAN_PATH.' '.GEPAN_PATH.'/GePan/scripts/runScheduler.pl -i '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/input/exporter.fas -o '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output -n '.$params->{'queueing'}.' -s '.$params->{'sorting'}."\n";
    print IN $statement;

    # copy result protein fastas back
    if($params->{'gestore'})
    {
        print IN "for i in `seq 1 ".$params->{'queueing'}."`;\n";
        print IN "do\n";
        print IN "\t";
        _printGeStoreCall(*IN, {'filename'=>"protein_scheduler",
                                'run'=>$params->{'script_id'},
                                'path'=>NODE_LOCAL_PATH.'/gepan/$JOB_ID/output/exporter.fas.$i',
                                'task'=>'$i',
                                'type'=>"l2r",
                                'format'=>"fasta" } );
        print IN "done\n";
        
        #print IN "tar -cf fileScheduler.tar --directory=".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/output/ .'."\n";
        #_printGeStoreCall(*IN, {'filename'=>"protein_scheduler.tar",
        #                        'run'=>$params->{'script_id'},
        #                        'path'=>"fileScheduler.tar",
        #                        'type'=>"l2r"} );
        #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."protein_scheduler.tar -D run=".$params->{'script_id'}." -D path=".NODE_LOCAL_PATH.'/gepan/$JOB_ID/output -D type=l2r -conf='.GESTORE_CONFIG."\n";
    } else {
        print IN "mv ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/output/* '.$params->{'data_files_dir'}."/cds/protein/tmp/\n";
    }

    # cleanup
    print IN "\n# Cleanup\n";
    print IN "rm ".NODE_LOCAL_PATH."/gepan/".'$JOB_ID/input/*'."\n";
    print IN 'rmdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/input'."\n";
    print IN 'rmdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output'."\n";
    print IN 'rmdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID'."\n";
    
    _timeStop($params, "fileScheduler", \*IN);
    
    close(IN);
}



=head2 B<_printSIngleShellCall(ref,GePan::ToolConfig,path)>

Prints the tool call into the shell script.

=cut

sub _printSingleShellCall{
    my ($params,$config,$file) = @_;

    # get input files that have to be copied to node
    my $inputPath = $params->{'data_files_dir'}."/".$config->getInputSequenceType()."/".$config->getInputType();
    $inputPath=~s/\/\//\//g;
    opendir(DIR,$inputPath) or $params->{'logger'}->LogError("startGePan::_printSIngleShellCall() - Failed to open directory $inputPath for reading");
    my @files = grep{(-f "$inputPath/$_")}readdir(DIR);
    closedir(DIR);
    $params->{'logger'}->LogError("More than one input file foun in directory $inputPath") unless (scalar(@files)==1);
    
    open(IN,">>$file") or $params->{'logger'}->LogError("startGePan::_printSIngleShellCall() - Failed to re-open shell file $file for writing");
   
    $inputPath.= "/".$files[0];
    $inputPath=~s/\/\//\//g;
    
    _timeStart($params, $config->getID(), \*IN);

    # create output directory
    print IN "\n# Create working directory for job\n";
    print IN "if [ ! -d ".NODE_LOCAL_PATH."/gepan ]\nthen\n\tmkdir -m 777 ".NODE_LOCAL_PATH."/gepan\nfi\n";
    print IN "cd ".NODE_LOCAL_PATH."/gepan\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID'."\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/input'."\n";
    print IN 'mkdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output'."\n";

    # copy input file to node
    print IN "\n# Copy input files to node\n";
    my $input_file_path = $params->{'data_files_dir'}."/".$config->getInputSequenceType()."/".$config->getInputType(); 
    my $input_file = "input.fas";

    if($params->{'gestore'})
    {
        #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."$input_file -D run=".$params->{'script_id'}." -D path=$input_file_path/$input_file -D type=l2r -D format=fasta -conf=".GESTORE_CONFIG."\n";
        _printGeStoreCall(*IN, {'file'=>$input_file, 'run'=>$params->{'script_id'}, 'path'=>$input_file_path."/".$input_file, 'type'=>"l2r", 'format'=>'fasta'});
        _printGeStoreCall(*IN, {'file'=>$input_file, 'run'=>$params->{'script_id'}, 'type'=>"r2l"});
        #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."$input_file -D run=".$params->{'script_id'}." -D path=".NODE_LOCAL_PATH."/gepan/\$JOB_ID/input/ -D type=r2l -conf=".GESTORE_CONFIG."\n";
        print IN "mv $input_file ".NODE_LOCAL_PATH."/gepan/\$JOB_ID/input/$input_file\n";
    } else {
        print IN "cp $input_file_path/$input_file ".NODE_LOCAL_PATH.'/gepan/$JOB_ID/input/'."\n";
    }

    # get paths and class of parser
    my ($toolClass,$toolParams) = _prepareTool($params,$config);

    # Parameter for tool on node
    my %nodeParams = %$toolParams;
    $nodeParams{'output_directory'} = NODE_LOCAL_PATH.'/gepan/$JOB_ID/output';
    $nodeParams{'input_file'} = NODE_LOCAL_PATH.'/gepan/$JOB_ID/input/'.$input_file; 

    # Change to output directory
    print IN "\n# Change to output directory\n";
    print IN 'cd '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output'."\n";

    print IN "\n# Execute tool\n";
    eval{
	_initializeTool($toolClass,\%nodeParams,*IN);
    };
    $params->{'logger'}->LogError("startGePan::_printSingleShellCall() - $@") if ($@);

    # move result file to users home directory structure
    print IN "\n# Move result files to users home\n";
    if($config->getID() eq "mga"){
    }
    else{
	if($params->{'gestore'})
        {
            my $filename = "input.fas.".$config->getID().".out";
            my $localFile = NODE_LOCAL_PATH.'/gepan/$JOB_ID/output/input.fas.'.$config->getID().'.out.predict';
            #print IN "hadoop jar ".GESTORE_PATH." org.diffdb.move -D file=".$params->{'script_id'}."".$filename." -D run=".$params->{'script_id'}." -D path=".$localFile." -D type=l2r -D format=glimmerpredict -conf=".GESTORE_CONFIG."\n";
            _printGeStoreCall(*IN, {'file'=>$filename, 'path'=>$localfile, 'run'=>$params->{'script_id'}, 'format'=>$config->getGsOutputFormat(), 'type'=>"l2r"});
        } else {
            print IN 'mv '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output/input.fas.'.$config->getID().'.out.predict '.$params->{'tool_files_dir'}."/".$config->getID()."/input.fas.".$config->getID().".out\n";
        }
    }
    
    print "Config ID: ".$config->getID()."\n";
    print "Input type: ".$config->getInputType()."\n";
    print "Input type: ".$config->getInputSequenceType()."\n";
    print "Output type: ".$config->getOutputType()."\n";
    print "Output format: ".$config->getOutputFormat()."\n";
    print "Output sequence type: ".$config->getOutputSequenceType()."\n";

    # cleanup
    print IN "\n# Cleanup\n";
    print IN 'rm '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output/*'."\n";
    print IN 'rmdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/output'."\n"; 
    print IN 'rm '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/input/*'."\n";
    print IN 'rmdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID/input'."\n";
    print IN 'rmdir '.NODE_LOCAL_PATH.'/gepan/$JOB_ID'."\n"; 
    
    _timeStop($params, $config->getID(), \*IN);
    
    close(IN);
}


=head2 B<_prepareTool(GePan::ToolConfig)>

Returns string of tool-module name and parameter hash for a given tool and the user-defined additional parameter.

Returns GePan::Tool object

=cut

sub _prepareTool{
    my ($params,$config) = @_;

    my $toolParams = {};

    # get tool input file and directory
    my $inputDir = NODE_LOCAL_PATH.'/gepan/$JOB_ID/input'."\n";
    my ($iD,$inputFile) = _getToolInput($params,$config);
    $toolParams->{'input_file'} = $inputDir."/".$inputFile;

    # check if output directory exists. If so die otherwise create it
    $toolParams->{'output_dir'} = _prepareToolOutputDir($params,$config);

    # create name of output file. Output files name = INPUT_FILE.TOOL_NAME.out
    #  In case input file was already created just substitute TOOL_NAME
    $toolParams->{'output_file'} = _createToolOutputName($params,$config,$inputFile);
    $toolParams->{'logger'} = $params->{'logger'};

    # set user-defined tool parameter
    $toolParams->{'parameter'} = _createParameter($params,$config);

    # create tool class
    my $toolClass;

    ### BAD BAD BAD PROGRAMMING.... BUT LAZYNESS SUCCEEDED ... SHOULD BE CHANGED SOME TIME
    # checks if one blast-program is called and changes class name
    if($config->getID()=~m/blast/i){
        $toolClass = 'GePan::Tool::Annotation::Blast';
        $toolParams->{'program'} = $config->getID();
    }
    else{
        $toolClass = 'GePan::Tool::'.ucfirst($config->getType())."::".ucfirst($config->getID());
    }

    return ($toolClass,$toolParams);

}


=head2 B<_createParameter(GePan::ToolConfig)>

Returns additional tool parameter hash.

=cut

sub _createParameter{
    my ($params,$config)  = @_;

    if($params->{'tools'}->{$config->getID()}){
        my $userParams = "";
        my $tp = {};
        foreach(keys(%{$params->{'tools'}->{$config->getID()}})){
            next if $_ eq 'id';
            $tp->{$_} = $params->{'tools'}->{$config->getID()}->{$_};
        }
        return $tp;
    }
    return 0;
}






=head2 B<_initializeTool(class,params,obj)>

Takes a class string and a parameter hash and initializes tool 'obj' of given class.

=cut

sub _initializeTool{
    my ($class,$params,$fh,$queueing) = @_;
    #print Dumper($params);
    my $module = $class;
    $module=~s/::/\//g;
    require "$module.pm";
    my $obj = $class->new();
    $obj->setParams($params);
    print $fh $obj->_getExecuteStatement($queueing)."\n";
}


=head2 B<_printSingleShellHeader(ref,GePan::ToolConfig,path)>

Writes the header fora job that is a single job submitted to cluster.

=cut

sub _printSingleShellHeader{
    my ($params,$config,$file,$exporter,$logDir) = @_;

    open(IN,">$file") or $params->{'logger'}->LogError("startGePan::_printSingleShellHeader() - Could not open shell script for writing header");
    print IN '#!/bin/sh'."\n\n";
    print IN '# This script was automatically generated by the \'GEne Prediction and ANnotation\' pipeline GePan developed by Tim Kahlke.'."\n";
    print IN '# Don\'t modify this script except you know what you are doing.'."\n\n";
    print IN '#$ -S /bin/bash'."\n";
    if($exporter){
	my $name = $config->getID().".exporter"."_".$params->{'script_id'};
	print IN '#$ -N '."$name\n";
	print IN '#$ -o '.$logDir.'/'.$config->getID().'.exporter.log'."\n";
    }
    else{
	print IN '#$ -N '.$config->getID()."_".$params->{'script_id'}."\n";
	print IN '#$ -o '.$params->{'log_files_dir'}.'/'.$config->getID().'.log'."\n";
    }
    print IN '#$ -j y'."\n\n";
    close(IN);
} 



=head1 INTERNAL METHODS


=head2 B<_createPipeline(ref)>

Creates the pipeline and performs some checks.

=cut

sub _createPipeline{
    my $params = shift;
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
            $params->{'logger'}->LogError("startGePan::_prepareGePan() - Inappropriate given input sequence type: given \'".$params->{'sequence_input_type'}."\' but \'".$config->getInputSequenceType()."\' is required for tool ".$config->getID());
        }
        elsif(lc($config->getInputType()) ne lc($params->{'sequence_type'})){
            $params->{'logger'}->LogError("startGePan::_prepareGePan() - Inappropriate given input sequence type \'".$params->{'sequence_type'}."\' for tool ".$config->getID());
        }
    }
}


=head2 B<_checkScriptParameter()>

Evaluaters user defined parameter.

=cut

sub _checkScriptParameter{
    my $params = shift;

    if (!$params->{'sorting'}){$params->{'sorting'} = 1;}

    if($params->{'old_work_dir'}){
        $params->{'logger'}->LogError("Given old working directory does not exist.") unless (-d $params->{'old_work_dir'});
    }
    elsif(!$params->{'exporter_type'}||!$params->{'work_dir'}||!$params->{'fasta'}||!$params->{'sequence_input_type'}||!$params->{'sequence_type'}){
        _usage();
    }
    elsif(($params->{'sequence_type'} ne 'nucleotide')&&($params->{'sequence_type'} ne 'protein')){
        _usage();
    }
    elsif(!($params->{'queueing'})){
	_usage();
    }

}


=head2 B<_registerTools()>

Reads in all ToolDefinition files and creates ToolRegister Object.

=cut

sub _registerTools{
    my $params= shift;


    # create hash-ref for tools of form {toolName=>{parameter_name=>$parameter_value}}
    $params->{'logger'}->LogStatus("Parsing user defined tool parameter");
    $params->{'tools'} = _createToolHash($params->{'tool_string'},$params->{'logger'});

    # register all defined tools
    $params->{'logger'}->LogStatus("Registering tools");
    my $toolRegister = GePan::ToolRegister->new();
    my $p = {'logger'=>$params->{'logger'}};
    if($params->{'tool_def_dir'}){
        $p->{'config_dir'} = $params->{'tool_def_dir'};
    }
    else{
        $p->{'config_dir'} = GEPAN_PATH."/GePan/ToolDefinitions";
    }
 
    warn($p->{'config_dir'});
    $toolRegister->setParams($p);
    $toolRegister->register();
    return $toolRegister;
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
    if($params->{'db_def_dir'}){
        $p->{'config_dir'} = $params->{'db_def_dir'};
    }
    else{
        $p->{'config_dir'} = GEPAN_PATH."/GePan/DatabaseDefinitions";
    }
    $databaseRegister->setParams($p);
    $databaseRegister->register();
    return $databaseRegister;
}



=head2 B<_createDirectoryStructure(ref)>

Creates all directories needed in the home directory of the user.

Directories created:

work_dir: parent directory for a GePan run

WORK_DIR/tool: directory for result files of all tools. Each tool has an own directory in here

WORK_DIR/data: directory where input data is stored, e.g. contig or cds fasta files. Files are written to sub-directories 'nucleotide' or 'protein'

WORK_DIR/result: directory for result files.

WORK_DIR/logs: directory for log and error files for GePan and all nodes.

WORK_DIR/shells: directory for shell files submitted to the cluster

=cut

sub _createDirectoryStructure{
    my $params = shift;

    # create directory for GePan run
    my $time = time;
    my $work_dir=$params->{'work_dir'}."/$time";
    $work_dir=~s/\/\//\//g;
    $params->{'work_dir'} = $work_dir;
    my $e = system("mkdir ".$params->{'work_dir'});
    die "Failed to create parent directory $work_dir" if $e;

    # create directory for log files
    my $logs = $params->{'work_dir'}."/logs";
    $logs=~s/\/\//\//g;
    $params->{'log_files_dir'} = $logs;
    my $e = system("mkdir ".$params->{'log_files_dir'});
    die "Failed to create directory for log files $logs" if $e;

    # create logger
    my $logger = GePan::Logger->new();
    $logger->setStatusLog(($params->{'log_files_dir'})."/GePan.log");
    $params->{'logger'} = $logger;

    # get starting time for run-time estimation 
    $params->{'running_since'} = time;
    $logger->LogStatus("GePan started at ".$params->{'running_since'});

    # create directory for result files
    my $resultPath = $params->{'work_dir'}."/results";
    $resultPath=~s/\/\//\//g;
    $params->{'logger'}->LogStatus("Creating directory for result files: $resultPath");
    $e = system("mkdir $resultPath");
    $params->{'logger'}->LogError("startGePan::_createDirectoryStructure() - Failed to create directory for result files \'$resultPath\'") if $e;
    $params->{'result_files_dir'} = $resultPath;

    # create data directory
    my $dataPath = $params->{'work_dir'}."/data";
    $dataPath=~s/\/\//\//g;
    $params->{'logger'}->LogStatus("Creating directory for data files: $dataPath");
    $e = system("mkdir $dataPath");
    $params->{'logger'}->LogError("startGePan::_createDirectoryStructure() - Failed to create directory for data files \'$dataPath\'") if $e;
    $params->{'data_files_dir'} = $dataPath;

    # create directory for shell files
    my $shellPath = $params->{'work_dir'}."/shells";
    $shellPath=~s/\/\//\//g;
    $params->{'logger'}->LogStatus("Creating directory for shell files: $shellPath");
    $e = system("mkdir $shellPath");
    $params->{'logger'}->LogError("startGePan::_createDirectoryStructure() - Failed to create directory for shell files \'$shellPath\'") if $e;
    $params->{'shell_files_dir'} = $shellPath;

    # create directory for tool result files
    my $toolPath = $params->{'work_dir'}."/tools";
    $toolPath=~s/\/\//\//g;
    $params->{'logger'}->LogStatus("Creating directory for tools files: $toolPath");
    $e = system("mkdir $toolPath");
    $params->{'logger'}->LogError("startGePan::_createDirectoryStructure() - Failed to create directory for shell files \'$toolPath\'") if $e;
    $params->{'tool_files_dir'} = $toolPath;
    return $time;
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
        $logger->LogError("startGePan::_createToolHash - Wrong number of elements in split of tool string.") unless scalar(@toolSplit)<=2;
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
                $logger->LogError("startGePan::_createToolHash() - Number of elements > 2 for parameter split.") unless ((scalar(@parameterSplit))&&(scalar(@parameterSplit)<=2));

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



=head2 B<_createSubDirectories()>

Creates sub directories for tools and input files

=cut

sub _createSubDirectories{
    my $params = shift;
    
    my $pipeline = $params->{'pipeline'};

    # create sub-directory for given input fasta 
    my $inputPath = $params->{'data_files_dir'}."/".(lc($params->{'sequence_input_type'}));
    $inputPath=~s/\/\//\//g;
    $params->{'logger'}->LogStatus("Creating directory for given input file data type: $inputPath");
    my $e = system("mkdir $inputPath");
    $params->{'logger'}->LogError("startGePan::_createSubDirectories() - Failed to create directory for input data types.") if $e;
    $inputPath .="/".(lc($params->{'sequence_type'}));
    $inputPath=~s/\/\//\//g;
    $params->{'logger'}->LogStatus("Creating directory for given input file sequence data type: $inputPath");
    $e = system("mkdir $inputPath");
    $params->{'logger'}->LogError("startGePan::_createSubDirectories() - Failed to create directory for input data types.") if $e;

    # copy input files to data-type directory
    $inputPath.="/input.fas";
    my $inputFile = $params->{'fasta'};
    my $copyStatement = "cp ".$params->{'fasta'}." $inputPath";
    $params->{'logger'}->LogStatus("Copy input files.");
    $e = system($copyStatement);
    $params->{'logger'}->LogError("startGePan::_createSubDirectories() - Failed to copy input files to data-type directory.") if $e;

    # create sub directories for each tool and tool input and output files
    foreach my $level (@$pipeline){
	foreach my $config (@$level){
	    # create directory for tool result files
	    my $toolResultDir = $params->{'tool_files_dir'}."/".lc($config->getID());
	    $toolResultDir=~s/\/\//\//g;
	    $e = system("mkdir $toolResultDir");
	    $params->{'logger'}->LogError("startGePan::_createSubDirectories() - Failed to create directory for tool outpu files $toolResultDir") if ($e);

	    # check if directory for tool input files exists
	    my $toolInputDir = $params->{'data_files_dir'}."/".$config->getInputSequenceType()."/".$config->getInputType();
	    $toolInputDir=~s/\/\//\//g;
	    $params->LogError("startGePan::_createSubDirectories() - Input directory $toolInputDir for tool ".$config->getID()." does not exist!") unless (-d $toolInputDir);

	    # create directories for files that are used as input files for following tools
	    if(($config->getOutputSequenceType())&&(!(ref($config->getOutputSequenceType())))){
		my $toolOutputDir = $params->{'data_files_dir'}."/".lc($config->getOutputSequenceType());
		if(!(-d($toolOutputDir))){
		    $params->{'logger'}->LogStatus("Creating directory for data files $toolOutputDir");
		    $e = system("mkdir $toolOutputDir");
		    $params->{'logger'}->LogError("GePan::_createSubDirectories() - Failed to create directory for data files $toolOutputDir") if ($e);
		}
		# get all output types of tool
		my @split = split(",",$config->getOutputType());
		foreach(@split){
		    my $toolOutputType = $toolOutputDir."/".$_;
		    $toolOutputType=~s/\/\//\//g;
		    if(!(-d($toolOutputType))){
			$params->{'logger'}->LogStatus("Creating directory for data files $toolOutputType");
			$e = system("mkdir $toolOutputType");
			$params->{'logger'}->LogError("GePan::_createSubDirectories() - Failed to create directory for data files $toolOutputType") if ($e);
		    }
		    # create tmp directories for filescheduler result files
		    $toolOutputType.="/tmp";
		    if(!(-d($toolOutputType))){
                        $params->{'logger'}->LogStatus("Creating directory for data files $toolOutputType");
                        $e = system("mkdir $toolOutputType");
                        $params->{'logger'}->LogError("GePan::_createSubDirectories() - Failed to create directory for data files $toolOutputType") if ($e);
                    }
		}
	    }
	}
    }
}


=head2 B_writeStartParameter(ref)>

Writes al given start parameter to xml file.

=cut

sub _writeStartParameter{
    my $params = shift;

    my $out = $params->{'work_dir'}."/parameter.xml";
    $params->{'logger'}->LogStatus("Writing given parameter to parameter file $out");
    open(OUT,">$out") or $params->{'logger'}->LogError("startGePan::_writeStartParameter() - Failed to open file $out for writing.");

    print OUT "<Parameter>\n";
    print OUT "\t<work_dir>".$params->{'work_dir'}."</work_dir>\n";
    print OUT "\t<data_files_dir>".$params->{'data_files_dir'}."</data_files_dir>\n";
    print OUT "\t<tool_files_dir>".$params->{'tool_files_dir'}."</tool_files_dir>\n";
    print OUT "\t<result_files_dir>".$params->{'result_files_dir'}."</result_files_dir>\n";
    print OUT "\t<shell_files_dir>".$params->{'shell_files_dir'}."</shell_files_dir>\n";
    print OUT "\t<log_files_dir>".$params->{'log_files_dir'}."</log_files_dir>\n";
    print OUT "\t<fasta>".$params->{'fasta'}."</fasta>\n";
    print OUT "\t<sequence_input_type>".$params->{'sequence_input_type'}."</sequence_input_type>\n";
    print OUT "\t<sequence_type>".$params->{'sequence_type'}."</sequence_type>\n";
    print OUT "\t<tool_string>".$params->{'tool_string'}."</tool_string>\n";
    print OUt "\t<taxa>".$params->{'taxa'}."</taxa>\n";
    print OUT "\t<exporter_type>".$params->{'exporter_type'}."</exporter_type>\n";
    print OUT "\t<tool_def_dir>".$params->{'tool_def_dir'}."</tool_def_dir>\n";
    print OUT "\t<db_def_dir>".$params->{'db_def_dir'}."</db_def_dir>\n";
    print OUT "\t<queueing>".$params->{'queueing'}."</queueing>\n";
    print OUT "</Parameter>";

    close(OUT);
}


=head2 B<_getToolInput($gePan::ToolConfig)>

Returns directory and file name a tool has to run on (was running on).

=cut

sub _getToolInput{
    my ($params,$config) = @_;

    # get parent input directory
    my $parentInputDir = NODE_LOCAL_PATH.'/gepan/$JOB_ID/input/';
    $parentInputDir=~s/\/\//\//g;

    my $parentFileName = "";
    if($config->getType() eq 'prediction'){
	$parentFileName = "input.fas";
    }
    return ($parentInputDir,$parentFileName);
}




=head2 B<_getParserClass(GePan::ToolConfig)>

Returns the class name of parser for output files of a given tool.

=cut

sub _getParserClass{
    my $config = shift;
    my $parserClass;

#    if($config->getType() eq 'prediction'){
#         $parserClass = "GePan::Parser::".ucfirst($config->getType())."::".ucfirst($config->getOutputSequenceType())."::".ucfirst($config->getID());
#    }
#    else{
#        $parserClass = "GePan::Parser::".ucfirst($config->getType())."::".ucfirst($config->getID());
#    }
#    return $parserClass;

    return $config->getParser();

}


sub _getParserInput{
    my ($params,$config) =@_;

    my $parserInputDir = $params->{'tool_files_dir'}."/".$config->getID();
    $parserInputDir=~s/\/\//\//g;

    my $parserInputFile;
    if($config->getType() eq "prediction"){
	$parserInputFile = "prediction_test.out";
    }
    return ($parserInputDir,$parserInputFile);
}


=head2 B<_prepareToolOutputDir()>

Create output directory for tool and return path to it.

=cut

sub _prepareToolOutputDir{
    my ($params,$config) = @_;

    my $outputDir = $params->{'tool_files_dir'}."/".$config->getID();
    $outputDir=~s/\/\//\//g;
    if(!-d $outputDir){
        $params->{'logger'}->LogStatus("Creating output directory \'$outputDir\' for result files of tool ".$config->getID());
        my $e = system("mkdir $outputDir");
        $params->{'logger'}->LogError("GePan::startGePan::_prepareTool() - Failed to create output directory $outputDir") if $e;
    }
    return $outputDir;
}



=head2 B<_createToolOutputName($config,inputFileName)>

Creates name of an output file of a tool.

=cut

sub _createToolOutputName{
    my ($params,$config,$inputFileName) = @_;

    my $outputFile;
    my @pathSplit = split("/",$inputFileName);
    my @nameSplit = split(".",$pathSplit[-1]);
    if(scalar(@nameSplit)==3){
        $outputFile = $nameSplit[0].".".lc($config->getID()).".out";
    }
    else{
        $inputFileName=~s/\./_/g;
        $outputFile = $pathSplit[-1].".".lc($config->getID()).".out";
    }
    return $outputFile;
}



=head2 B<prepareToolInput($config)>

Returns 

=head2 B<_prepareToolExporter($config)>

Prepares exporter for a tool that containes a defined output (fasta).

Return parserClassName,parserParams and exporterOutputDir.

=cut

sub _prepareToolExporter{
    my ($params,$config) = @_;

    my ($parserClass,$parserParams) = _prepareToolParser($params,$config);

    # get path to output directory for exporter    
    my $exporterOutDir = $params->{'data_files_dir'}."/".$config->getOutputSequenceType();
    $exporterOutDir=~s/\/\//\//g;

    return ($parserClass,$parserParams,$exporterOutDir);
}



=head2 B<_prepareToolParser(GePan::ToolConfig)>

Configures output-file parser for given tool. Returns a hash of parameter and the class name of the parser.

=cut

sub _prepareToolParser{
    my ($params,$config) = @_;

    # create class for parser of tool
    my $parserClass = _getParserClass($config);

    
    # get tool output file for parser
    my ($parserInputDir,$parserInputFile) = _getParserInput($params,$config);

    # get parent input data
    my ($parentDir,$parentFile) = _getToolInput($params,$config);

    # get parent sequences
    my $parentInputFile = "$parentDir/$parentFile";
    $parentInputFile=~s/\/\//\//g;
    my $parentSeqs = _getParentSequences($params,$config,$parentInputFile);

    # create Parser params
    $parserInputFile = "$parserInputDir/$parserInputFile";
    $parentInputFile=~s/\/\//\//g;
    my $parserParams=  {'parent_sequences' => $parentSeqs,
                        logger=>$params->{'logger'},
                        file=>$parserInputFile};

    return ($parserClass,$parserParams);
}



=head2 B<_getParentSequences($config,$parentFileName)>

Returns GePan::Collection object of the fasta sequences the tool for the parser was run on.

=cut

sub _getParentSequences{
    my ($params,$config,$parentFileName) = @_;
}


=head2 B<_checkToolDBs()>

Checks if chosen tools and databases are consistent. 

=cut

sub _checkToolDBs{
    my $params = shift;

    # get collection fo user defined tools
    my $userTools = GePan::Collection::ToolConfig->new();
    while(my $config = $params->{'tool_register'}->getCollection()->getNextElement()){
	if($params->{'tools'}->{$config->getID()}){
	    $userTools->addElement($config);
	}
    }

    # check if tools have at least one user defined database
    while(my $config = $userTools->getNextElement()){
	next unless $config->getType() eq "annotation";
	next unless $config->getDBFormat();
	my $d = $params->{'db_register'}->getCollection()->getElementsByAttributeHash({sequence_type=>$config->getInputType(),database_format=>$config->getDBFormat()});

	my $dbs;
	if($params->{'taxa'}){
	    my @taxa = split(",",$params->{'taxa'});
	    $dbs = GePan::Collection::ToolConfig->new();
	    while(my $db_config = $d->getNextElement()){
                foreach(@taxa){
                    if($db_config->getDatabaseTaxon()=~m/$_/){
                        $dbs->addElement($db_config);
                    }
                }
            }
	}
	else{
	    $dbs = $d;
	}
	
	if(!($dbs->getSize())){
	    $params->{'logger'}->LogError("No databases of appropriate type or taxon found for tool ".$config->getID());
	}
	
    }
}




sub _usage{
    print STDOUT "\n ---- gepan ----\n";
    print STDOUT "Starts the GePan pipeline on a computer cluster using SGE (Sun Grid Engine).\n";
    print STDOUT "Parameter:\n";
    print STDOUT "w : Working directory.\n";
    print STDOUT "f : Fasta file of sequences or directory of fasta files\n";
    print STDOUT "p : String of tool names. String is of form \"tool_name:parameter_name1=parameter_value,paramter_name2=parameter_value;...\".\n";
    print STDOUT "    For parameter without value the value is to be left empty (for more information see gepan documentation).\n";
    print STDOUT "T : Type of input sequence, either nucleotide or protein\n";
    print STDOUT "S : Type of input sequences, e.g. contig or CDS.\n";
    print STDOUT "t : String of taxa seperated by \',\'. If set just databases of given taxa are searched.\n";
    print STDOUT "q : number of cores used\n";
    print STDOUT "s : Perform sorting to optimize runtime of tools running on multiple cores. Logical; yes or no (1/0). Sorts by default (1)\n";
    print STDOUT "o : Output generator of choice\n";
#    print STDOUT "    1 = Complete (human readable) output tab separated (GePan::Exporter::CompleteTabSeparated)\n";
    print STDOUT "    1a = Strict embl export using just qualifiers defined for embl format\n";
    print STDOUT "    1b = Embl export with qualifiers unique for each tool run (not conform with defined embl-format scheme)\n";
#    print STDOUT "    2 = Functional (human readable) output tab separated (GePan::Exporter::FunctionalTabSeparated)\n";
    print STDOUT "    2 = Simple output in xml-format (GePan::Exporter::XML::SimpleAnnotation)\n";
    print STDOUT "    3 = Tab delimited format supported by Metarep (GePan::Exporter::Metarep)\n";
#    print STDOUT "    4 = Complete project output in xml-format (GePan::Exporter::XML::Project)\n";
    print STDOUT "r : Old working directory. Parameter used for re-running the annotation on already run tools.\n";
    print STDOUT "d : Directory of tool definition files (optional, default: ../ToolDefinitions)\n";
    print STDOUT "b : Directory of database definition files (optional, default: ../DatabaseDefinitions)\n";
    print STDOUT "P : Include performance information in shell scripts (optional)\n";
    print STDOUT "R : Create a script to submit the jobs instead of running them (optional)\n";
    print STDOUT "G : Use GeStore, set taxon (optional)\n";
    print STDOUT "\n";
    print STDOUT "\n";
    exit;
}





