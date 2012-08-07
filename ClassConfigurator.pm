package GePan::ClassConfigurator;

use strict;
use Data::Dumper;


=head1 NAME

GePan::ClassConfigurator

=head1 DESCRIPTION

Class for constructing class-names, parameter hashes or directory paths based on GePan::ToolConfig objects.

=head1 ATTRIBUTES

work_dir: working directory of this gepan-run

logger: GePan::Logger object

tool_files_dir: output directories for the tools

data_files_dir: directory of data files, e.g. contig.fas or read.fas files

tool_parameter: hash of additional parameter of tools (user-defined) 

cpu: number of cpus used

=head1 CONSTRUCTOR

=head1 B<new()>

Returns an empty GePan::ClassConfigurator object.

=cut

sub new{
    my $class = shift;
    my $self = {};
    return(bless($self,$class));
}



=head1 METHODS



=head2 B<_prepareTool(GePan::ToolConfig)>

Returns string of tool-module name and parameter hash for a given tool and the user-defined additional parameter.

Returns GePan::Tool object

=cut

sub prepareTool{
    my ($self,$config) = @_;

    my $toolParams = {};

    # get tool input file and directory
    my ($inputDir,$inputFile) = $self->getToolInput($config); 
    $toolParams->{'input_file'} = $inputDir."/".$inputFile;

    # check if output directory exists. If so die otherwise create it
    $toolParams->{'output_dir'} = $self->prepareToolOutputDir($config);
    $toolParams->{'cpu'} = $self->{'cpu'}?$self->{'cpu'}:1;


    # create name of output file. Output files name = INPUT_FILE.TOOL_NAME.out
    #  In case input file was already created just substitute TOOL_NAME
    $toolParams->{'output_file'} = $self->createToolOutputName($config,$inputFile);
    $toolParams->{'logger'} = $self->{'logger'};

    # set user-defined tool parameter
    $toolParams->{'parameter'} = $self->_createParameter($config);

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
    my ($self,$config)  = @_;
    
    if($self->{'tool_parameter'}->{$config->getID()}){
        my $userParams = "";
        my $tp = {};
        foreach(keys(%{$self->{'tool_parameter'}->{$config->getID()}})){
            next if $_ eq 'id';
            $tp->{$_} = $self->{'tool_parameter'}->{$config->getID()}->{$_};
        }
	return $tp;
    }
    return 0;
}


=head2 B<createToolOutputName($config,inputFileName)>

Creates name of an output file of a tool.

=cut

sub createToolOutputName{
    my ($self,$config,$inputFileName) = @_;
   
    my $outputFile;
    my @nameSplit = split(".",$inputFileName);
    if(scalar(@nameSplit)==3){
        $outputFile = $nameSplit[0].".".lc($config->getID()).".out";
    }
    else{
        $inputFileName=~s/\./_/g;
        $outputFile = $inputFileName.".".lc($config->getID()).".out";
    }

    return $outputFile;
}


=head2 B<prepareToolOutputDir()>

Create output directory for tool and return path to it.

=cut

sub prepareToolOutputDir{
    my ($self,$config) = @_;

    my $outputDir = $self->{'tool_files_dir'}."/".$config->getID();
    $outputDir=~s/\/\//\//g;
    if(!-d $outputDir){
	$self->{'logger'}->LogStatus("Creating output directory \'$outputDir\' for result files of tool ".$config->getID());
	my $e = system("mkdir $outputDir");
	$self->{'logger'}->LogError("GePan::ClassConfigurator::_prepareTool() - Failed to create output directory $outputDir") if $e;
    }
    return $outputDir;
}


=head2 B<prepareToolInput($config)>

Returns 

=head2 B<prepareToolExporter($config)>

Prepares exporter for a tool that containes a defined output (fasta).

Return parserClassName,parserParams and exporterOutputDir.

=cut

sub prepareToolExporter{
    my ($self,$config) = @_;

    my ($parserClass,$parserParams) = $self->prepareToolParser($config);

    # get path to output directory for exporter    
    my $exporterOutDir = $self->{'data_files_dir'}."/".$config->getOutputSequenceType();
    $exporterOutDir=~s/\/\//\//g;
    $self->_createDir($exporterOutDir);

    # create directories for exporter output files
    my @outputTypes = split(",",$config->getOutputType());
    foreach(@outputTypes){
        my $exporterTypeOutDir = $exporterOutDir."/".lc($_);
        $exporterTypeOutDir=~s/\/\//\//g;
	$self->_createDir($exporterTypeOutDir);
    }

    return ($parserClass,$parserParams,$exporterOutDir);

}


=head2 B<_createDir(path)>

Takes a directory path and creates it if not existing.

=cut

sub _createDir{
    my ($self,$path)  =@_;
    if(!(-d($path))){
        $self->{'logger'}->LogStatus("Create directory $path");
        my $e = system("mkdir $path");
        $self->{'logger'}->LogError("ToolConfigurator::_createDir() - Failed to create directory $path") if $e;
    }
}



=head2 B<prepareToolParser(GePan::ToolConfig)>

Configures output-file parser for given tool. Returns a hash of parameter and the class name of the parser.

=cut

sub prepareToolParser{
    my ($self,$config) = @_;
    
    # create class for parser of tool
    my $parserClass = $self->getParserClass($config);

    # get tool output file for parser
    my ($parserInputDir,$parserInputFile) = $self->getParserInput($config);    

    # get parent input data
    my ($parentDir,$parentFile) = $self->getToolInput($config);

    # get parent sequences
    my $parentInputFile = "$parentDir/$parentFile";
    $parentInputFile=~s/\/\//\//g;
    my $parentSeqs = $self->getParentSequences($config,$parentInputFile);

    # create Parser params
    $parserInputFile = "$parserInputDir/$parserInputFile";
    $parentInputFile=~s/\/\//\//g;
    my $parserParams=  {'parent_sequences' => $parentSeqs,
			logger=>$self->{'logger'},
			file=>$parserInputFile};

    return ($parserClass,$parserParams);
}


=head2 B<getParentSequences($config,$parentFileName)>

Returns GePan::Collection object of the fasta sequences the tool for the parser was run on.

=cut

sub getParentSequences{
    my ($self,$config,$parentFileName) = @_;
    
    my $parentParser = GePan::Parser::Input::Fasta->new();
    $parentParser->setParams({type=>$config->getInputSequenceType(),
                              file=>$parentFileName,
                              logger=>$self->{'logger'}});
    $parentParser->parseFile();

    return $parentParser->getCollection();
}


=head2 B<getToolInput($gePan::ToolConfig)>

Returns directory and file name a tool has to run on (was running on).

=cut

sub getToolInput{
    my ($self,$config) = @_;
    
    # get parent input directory
    my $parentInputDir = $self->{'data_files_dir'}."/".lc($config->getInputSequenceType())."/".lc($config->getInputType());
    $parentInputDir=~s/\/\//\//g;
    $self->{'logger'}->LogError("GePan::ToolConfigurator::getToolInput() - Directory of tool input files \'$parentInputDir\' does not exist.") unless (-d $parentInputDir);

    # get parent input file
    opendir(IN,$parentInputDir) or $self->{'logger'}->LogError("startStandAlone::_prepareToolExporter() - Failed to open directory $parentInputDir");
    my @parentFiles = grep{-f "$parentInputDir/$_"}readdir(IN);
    $self->{'logger'}->LogError("startStandAlone::_prepareToolExporter() - More than one result file for tool".$config->getID()." found.") unless scalar(@parentFiles)==1;
    my $parentFileName = $parentFiles[0];
    return ($parentInputDir,$parentFileName);
}



=head2 B<getToolOutput(GePan::ToolConfig)>

Returns the output directory and output file name of a given tool.

=cut

sub getParserInput{
    my ($self,$config) =@_;

    my $parserInputDir = $self->{'tool_files_dir'}."/".$config->getID();
    $parserInputDir=~s/\/\//\//g;

    opendir(IN,$parserInputDir) or $self->{'logger'}->LogError("ToolConfigurator::getToolDir() - Failed to open directory $parserInputDir");
    my $tmpName = $config->getID().".out";
    my @files = grep{$_=~m/$tmpName/}readdir(IN);
    closedir(IN);

    $self->{'logger'}->LogError("ToolConfigurator::() - More or less than one result file for tool ".$config->getID()." found.") unless scalar(@files)==1;
    my $parserInputFile =$files[0];
    return ($parserInputDir,$parserInputFile);
}


=head2 B<getParserClass(GePan::ToolConfig)>

Returns the class name of parser for output files of a given tool.

=cut

sub getParserClass{
    my ($self,$config) = @_;
    my $parserClass;

    if($config->getType() eq 'prediction'){
         $parserClass = "GePan::Parser::".ucfirst($config->getType())."::".ucfirst($config->getOutputSequenceType())."::".ucfirst($config->getID());
    }
    else{
        $parserClass = "GePan::Parser::".ucfirst($config->getType())."::".ucfirst($config->getID());
    }
    return $parserClass;
}

    
=head1 GETTER & SETTER METHODS

=head2 B<setWorkDir(path)>

Sets working directory.

=cut

sub set{
    my ($self,$a) = @_;
    $self->{'work_dir'} = $a;
}


=head2 B<getWorkDir()>

Returns working directory path.

=cut

sub getWorkDir{
    my $self = shift;
    return $self->{'work_dir'};
}


=head2 B<setLogger(GePan::Logger)>

Sets the GePan::Logger object.

=cut

sub setLogger{
    my ($self,$a) = @_;
    $self->{'logger'} = $a;
}


=head2 B<getLogger()>

Returns GePan::Logger object.

=cut

sub getLogger{
    my $self = shift;
    return $self->{'logger'};
}


=head2 B<setToolFilesDir(path)>

Sets the directory where tool-files are saved.
 
=cut

sub setToolFilesDir{
    my ($self,$a) = @_;
    $self->{'tool_files_dir'} = $a;
}


=head2 B<getToolFilesDir()>

Returns directory where tool output files are stored.

=cut

sub getToolFilesDir{
    my $self = shift;
    return $self->{'tool_files_dir'};
}


=head2 B<setDataFilesDir(path)>

Sets the directory where data files are stored, e.g. contig or cds fasta files.

=cut

sub setDataFilesDir{
    my ($self,$a) = @_;
    $self->{'data_files_dir'} = $a;
}


=head2 B<getDataFilesDir()>

Returns the directory where data files are stored, e.g. contig or cds fasta files.

=cut

sub get{
    my $self = shift;
    return $self->{'data_files_dir'};
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


1;
