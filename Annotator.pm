package GePan::Annotator;

use strict;
use Data::Dumper;
use GePan::Config qw(DATABASE_PATH GEPAN_PATH);
use GePan::DatabaseRegister;
use GePan::AnnotationDBI;
use GePan::SequenceAnnotation;
use GePan::Logger;

=head1 NAME

GePan::Annotator

=head1 DESCRIPTION

Class for annotation of all sequences and sequence types.

=head1 ATTRIBUTES

work_dir: path to working directory

annotation_tools: GePan::Collection::ToolConfig object of tools that are used to annotate the set type of sequences

attribute_tools: GePan::Collection::ToolConfig object of cds attribute prediction tools

sequences: GePan::Collection::Sequence object of sequences to annotate

sequence_type: type of given sequences that have to be annotated

logger: GePan::Logger object

database_collection: GePan::Collection::Database object of all registered databases

=cut

=head1 CONSTRUCTOR

=head2 B<new(working_dir)>

Returns a GePan::Annotator object

=cut

sub new{
    my ($class,$dir) = @_;
    my $self = {work_dir=>$dir};
    _load();
    return (bless($self,$class));
}

=head1 METHODS

=head2 B<annotate()>

Annotates all sequences annotation tools have been run for.

=cut

sub annotate{
    my $self = shift;

    $self->{'logger'}->LogError("Annotator::annotate() - No path to working directory given") unless $self->{'work_dir'};
    $self->{'logger'}->LogWarning("Annotator::annotate() - No GePan::Collection::ToolConfig set for annotation") unless $self->{'annotation_tools'};
    $self->{'logger'}->LogError("Annotator::annotate() - No sequences set for annotation") unless $self->{'sequences'};
    $self->{'logger'}->LogError("Annotator::annotaten() - No databases set for annotation process") unless $self->{'database_collection'};
    $self->{'logger'}->LogError("Annotator::annotate() - No type set for annotation process") unless $self->{'type'};

    # get all annotation tools
    my $annotationConfigs = $self->{'annotation_tools'};

    # if no annotation tools for this sequence_type are found print it to log and stop.
    if(!($annotationConfigs->getSize())){
	$self->{'logger'}->LogWarning("No annotation tools found for sequence type ".$self->{'sequence_type'});
	$self->{'logger'}->LogStatus("No annotation performed");
    }
    else{

	my $transferredTools = $self->{'annotation_tools'}->getElementsByAttributeHash({sub_type=>'transferred'});

	my $functionalTools = $self->{'annotation_tools'}->getElementsByAttributeHash({sub_type=>'functional'});
	$self->{'logger'}->LogStatus("Create functional annotations");

	# check if just transferred annotation tools are performed
	if(!($functionalTools->getSize())){
	    my $transferredTools = $self->{'annotation_tools'}->getElementsByAttributeHash({sub_type=>'transferred'});
	    $self->{'logger'}->LogStatus("Create transferred annotations");
	    $self->_annotateTransferred($transferredTools); 
	    $self->_convertTransferred2FinalConfidence();
	}	
	elsif(!($transferredTools->getSize())){
	    # check if just functional annotation tools are performed
	    my $functionalTools = $self->{'annotation_tools'}->getElementsByAttributeHash({sub_type=>'functional'});
	    $self->{'logger'}->LogStatus("Create functional annotations");
	    $self->_annotateFunctional($functionalTools);
	    $self->_convertFunctional2FinalConfidence();
	}
	else{
	    # get final confidence level from transferred and functional annotation
	    $self->{'logger'}->LogStatus("Create transferred annotations");
	    $self->_annotateTransferred($transferredTools);
	    $self->{'logger'}->LogStatus("Create functional annotations");
	    $self->_annotateFunctional($functionalTools);
	    $self->_getFinalConfidenceLevel();	
	}

	# get all hits of attribute-tools for cds
	my $attributeTools = $self->{'attribute_tools'};

	# get collection of all hits of attribute tools
	while(my $config = $attributeTools->getNextElement()){
	    my $aParams = $self->_prepareAttributeTool($config);
	    my $class = $config->getParser();
	    eval{_runAParser($self->{'logger'},$class,$aParams,$self->{'sequences'})};
	    $self->{'logger'}->LogError("GePan::Annotator::annotate() - Failed to run attribute tool parser - $@") if ($@);
	}
    }
}


=head2 B<_prepareAttributeTool()>

Prepares all attributes for the attribute-tool parser.

=cut

sub _prepareAttributeTool{
    my ($self,$config) = @_;

    my $params = {};

    my $input_dir = $self->{'work_dir'};
    $input_dir .="/tools/".$config->getID();
    $input_dir=~s/\/\//\//g;

    # get input file
    opendir(IN,$input_dir) or $self->{'logger'}->LogError("GePan::Annotator::_prepareAttributeTool() - Failed to open directory $input_dir for reading.");
    my @files = grep{-f "$input_dir/$_"}readdir(IN);
    $self->{'logger'}->LogError("GePan::Annotator::_prepareAttributeTool() - More than one input file found in directory $input_dir") if scalar(@files)!=1;    

    my $file = $input_dir.="/".$files[0];
    $input_dir=~s/\/\//\//g;

    $params->{'file'} = $file;
    $params->{'logger'} = $self->{'logger'};
    return $params;
}


=head2 B<_runAParser(class,params,collection)>

Runs a parser for attribute tool output files.Hit is added to sequence-annotation attribute_sequence collection. 

=cut

sub _runAParser{
    my ($logger,$class,$params,$collection) = @_;
  
    my $module = $class;
    $module=~s/::/\//g;
    require "$module.pm"; 
    my $parser = $class->new();
    $parser->setParams($params);
    $parser->parseFile();


    foreach my $s(@{$parser->getCollection()->getList()}){
	$logger->LogError("Element ".$s->getID()." does not exists or has no sequence_annotation object") unless ($collection->getElementByID($s->getID()));
	my $seq = $collection->getElementByID($s->getID());
	$seq->getAnnotation()->getAttributeCollection()->addElement($s);
    }
}



=head2 B<_annotateFunctional()>

Annotates the functional annotation of all sequences.

=cut

sub _annotateFunctional{
    my ($self,$tools) = @_;

    $self->{'logger'}->LogStatus("Get all functional hits");
    my $hitCollection = GePan::Collection::Hit->new();
    $self->_getFunctionalHits($hitCollection,$tools);

    $self->{'logger'}->LogStatus("Add annotations to found hits.");
    $self->_addAnnotations($hitCollection);

    $self->{'logger'}->LogStatus("Annotate hits with functional annotations");

    while(my $seq = $self->getSequences()->getNextElement()){

        # create annotation object for sequence
        my $annotation = $seq->getAnnotation()?$seq->getAnnotation():GePan::SequenceAnnotation->new();

        # get all hits of sequence
        my $seqHits = $hitCollection->getElementsByAttributeHash({query_name=>$seq->getID()});

        # if no hit for sequence was found set empty hit
        if(!($seqHits->getSize())){
            my $emptyHit = GePan::Hit::Pfam->new();
            my $emptyAnnotation = GePan::Annotation::Pfam->new();
            $emptyAnnotation->setDescription("No functional annotation found");
            $emptyAnnotation->setConfidenceLevel("3");
	    $emptyAnnotation->setID("empty");
	    $emptyAnnotation->setLogger($self->{'logger'}),
	    $emptyHit->setLogger($self->{'logger'}),
	    $emptyHit->setID("empty"),
            $emptyHit->setAnnotation($emptyAnnotation);
            $annotation->setFunctionalAnnotation($emptyHit);
            $seq->setAnnotation($annotation);
            next;
        }

        # get best functional hit
        my $best = $self->_getBestFunctionalHit($seqHits);
        # set confidence level of functional hit
        $self->_setFunctionalConfidence($best);
        # set functional annotation
        $annotation->setFunctionalAnnotation($best);
        $seq->setAnnotation($annotation);
    }
}




=head2 B<_annotateTransferred()>

Annotates the transferred annotation of all sequences.

=cut

sub _annotateTransferred{
    my ($self,$tools) = @_;
 
    $self->{'logger'}->LogStatus("Get all transferred hits");
    my $hitCollection = GePan::Collection::Hit->new();
    $self->_getTransferredHits($hitCollection,$tools);
    
    $self->{'logger'}->LogStatus("Add annotations to found hits.");
    $self->_addAnnotations($hitCollection);
    
    $self->{'logger'}->LogStatus("Annotate hits with transferred annotations");

    while(my $seq = $self->getSequences()->getNextElement()){
	# create annotation object for sequence
	$self->{'logger'}->LogError("Annotator::annotateTransferred - Sequence already annotated") if ($seq->getAnnotation());
	my $annotation = GePan::SequenceAnnotation->new();

	# get all hits of sequence
	my $seqHits = $hitCollection->getElementsByAttributeHash({query_name=>$seq->getID()});
	
	# if no hit for sequence was found set empty hit
	if(!($seqHits->getSize())){
	    my $emptyHit = GePan::Hit::Blast->new();
            my $emptyAnnotation = GePan::Annotation::Blast->new();
            $emptyAnnotation->setDescription("No transferred annotation found");
            $emptyAnnotation->setConfidenceLevel("4");
	    $emptyAnnotation->setLogger($self->{'logger'});
	    $emptyHit->setLogger($self->{'logger'});
	    $emptyHit->setID('empty');
	    $emptyHit->setAnnotation($emptyAnnotation);
	    $self->_setTransferredConfidence($emptyHit);
	    $annotation->setTransferredAnnotation($emptyHit);
	    $seq->setAnnotation($annotation);
	    next;
	}
	
	# get best transferred hit
	my $best = $self->_getBestTransferredHit($seqHits);
	# set confidence level of transferred hit
	$self->_setTransferredConfidence($best);
	# set transferred annotation
	$annotation->setTransferredAnnotation($best);
	$seq->setAnnotation($annotation);
    }

}


=head2 B<_getFinalConfidenceLevel()>

Creates final confidence levels for all sequences from transferred and functional annotation.

Confidence levels:

1:
    - Both hits are significant

2: 
    - Either transferred or functional hit found and that one is significant

3:  
    - Just insignificant hits found if any

=cut


sub _getFinalConfidenceLevel{
    my $self = shift;

    while(my $seq = $self->getSequences()->getNextElement()){
	my $t = $seq->getAnnotation()->getTransferredAnnotation()->getSignificance();
	my $f = $seq->getAnnotation()->getFunctionalAnnotation()->getSignificance();
    
	if(($t)&&($f)){
	    # level 1
	    $seq->getAnnotation()->setConfidenceLevel(1);
	    next;
	}
	elsif((($t)&&(!($f)))||(($f)&&(!($t)))){
	    # level 2
	    $seq->getAnnotation()->setConfidenceLevel(2);
            next;
	}
	else{
	    $seq->getAnnotation()->setConfidenceLevel(3);
	}
    }
}


=head2 B<_convertFunctional2FinalConfidence()>

If no functional annotation tools are run it converts the transferred annotation confidence levels to final confidence levels.

=cut

sub _convertFunctional2FinalConfidence{
    my $self = shift;

    while(my $seq = $self->getSequences()->getNextElement()){
	$seq->getAnnotation()->setConfidenceLevel($seq->getAnnotation()->getFunctionalAnnotation()->getAnnotation()->getConfidenceLevel());
    }
}



=head2 B<_convertTransferred2FinalConfidence()>

If no functional annotation tools are run it converts the transferred annotation confidence levels to final confidence levels.

=cut

sub _convertTransferred2FinalConfidence{
    my $self = shift;

    while(my $seq = $self->getSequences()->getNextElement()){
	my $t = $seq->getAnnotation()->getTransferredAnnotation()->getAnnotation()->getConfidenceLevel();
	if(($t==1)||($t==2)){
	    $seq->getAnnotation()->setConfidenceLevel(1);
            next;
	}
	elsif($t==3){
	    $seq->getAnnotation()->setConfidenceLevel(2);
            next;
	}
	else{
	    $seq->getAnnotation()->setConfidenceLevel(3);
            next;
	}
    }
}

=head2 B<_setTransferredConfidence(GePan::Hit)>

Sets the confidence level of a transferred annotation for given hit.

Confidence levels:

1: best hit of any tool above 50% identity or 60% similarity

2: best hit of any tool above 30% identity or 40% similarity

3: no significant hit found by any tool

4: no hit found

=cut

sub _setTransferredConfidence{
    my ($self,$hit) = @_;
    
    if(!($hit->getAnnotation)){
	my $a = GePan::Annotation::Blast->new();
	$a->setDescription("No description found for hit");
	$hit->setAnnotation($a);
    }

    if(!($hit->getSignificance())){
        $hit->getAnnotation->setConfidenceLevel("3");
    }    
    elsif(($hit->getPercentIdentity()>=50)||($hit->getPercentSimilarity()>=60)){
	$hit->getAnnotation->setConfidenceLevel("1");
    }
    elsif(($hit->getPercentIdentity()>=30)||($hit->getPercentSimilarity()>=40)){
	$hit->getAnnotation->setConfidenceLevel("2");
    }

}




=head2 B<_setFunctionalConfidence(GePan::Hit)>

Sets the confidence level of a functional annotation for given hit.

Confidence levels are:

1: hit is significant 

2: hit insignificant

3: no hit found

=cut

sub _setFunctionalConfidence{
    my ($self,$hit) = @_;

    if(!($hit->getAnnotation)){
        my $a = GePan::Annotation::Pfam->new();
        $a->setDescription("No description found for hit");
        $hit->setAnnotation($a);
    }


    if($hit->getSignificance()){
        $hit->getAnnotation->setConfidenceLevel("1");
    }
    else{
        $hit->getAnnotation->setConfidenceLevel("2");
    }

}




=head2 B<_getBestTransferredHit(GePan::Collection::Hit)>

Returns best of all hits in Collection. 

Best hit is considered the one only significant one or with highest identity....

=cut

sub _getBestTransferredHit{
    my ($self,$collection) = @_;

    my $highestIdent;
    my $highestSim;
    my $lowestE;
    
    while(my $hit = $collection->getNextElement()){
	$highestIdent = $hit unless (($highestIdent)&&($highestIdent->{'percent_identity'}>$hit->{'percent_identity'}));
        $highestSim = $hit unless (($highestSim)&&($highestSim->{'percent_similarity'}>$hit->{'percent_similarity'}));
        $lowestE = $hit unless (($lowestE)&&($lowestE->getEValue<$hit->getEValue()));
    }

    if($highestIdent->getID() ne $highestSim->getID()){
	$self->{'logger'}->LogWarning("Conflicting best hit results in similarity and identity. Highest Identity chosen!");
    }
    elsif($highestIdent->getID() ne $lowestE->getID()){
        $self->{'logger'}->LogWarning("Conflicting best hit results in e-value and identity. Highest Identity chosen!");
    }

    return $highestIdent;
}



=head2 B<_getBestFunctionalHit(GePan::Collection::Hit)>

Returns best of all hits in Collection. 

Best hit is considered the one with score ....

=cut

sub _getBestFunctionalHit{
    my ($self,$collection) = @_;

    my $score; 
    while(my $hit = $collection->getNextElement()){
        $score = $hit unless (($score)&&($score->getScore<$hit->getScore()));
    }

    return $score;
}



=head2 B<_addAnnotations(GePan::Collection::Hit)>

Sets annotation to all hits in given collection using GePan::AnnotationDBI for accessing annotation database files.

=cut

sub _addAnnotations{
    my ($self,$collection) = @_;

    # get all names of databases any hit was found in
    my $hitDBs = $collection->getDatabases();

    # annotate hits of each database
    foreach my $db(keys(%$hitDBs)){

        my $dbHits = $collection->getElementsByAttributeHash({database=>$db});
	my $dbConfig = $self->{'database_collection'}->getElementByID($db);
	$self->{'logger'}->LogError("Annotator::_addAnnotations() - No database config of id $db found.") unless $dbConfig;

        # creating path to annotation db files
	# Annotation files are put into directory DB_NAME.'_annoations';
        my @pathSplit = split(/\./,$dbConfig->getPath());
        
        #UGLY HACK!
        #Checks if the path is a real path
        #If not, it generates a new path for the gestore database used
        # TODO:
        # Generalize the database names in some way
        if(@pathSplit > 1) {
            pop(@pathSplit);
        } else {
            my @newPathSplit = split("/", join('', @pathSplit));
            my $dbName = pop(@newPathSplit);
            push(@newPathSplit, "uniprot");
            push(@newPathSplit, "uniprot_".$dbName."_gepan");
            $self->{'logger'}->LogWarning("Annotator::_addAnnotations() - getPath(): ".join(',', @newPathSplit)."\n");
            # splice(@newPathSplit, )
            @pathSplit = join("/", @newPathSplit);
        }

        my $dbPath = join(".",@pathSplit);
        $dbPath.="_annotations";
	# return in case no annotation directory was found, e.g. no annotation description etc can be done.
	if(!(-d($dbPath))){
	    $self->{'logger'}->LogWarning("Annotator::_addAnnotations() - Directory of annotation database file $dbPath does not exist.");
	    next;
	}	

	# get berkeleyDB module for reading annotations
        my $dbi = GePan::AnnotationDBI->new();
	$dbi->{'logger'} = $self->{'logger'};
        $dbi->setDB($dbPath);

        # sort hits by ID
        my $sortHits = {};
        foreach my $hit(@{$dbHits->getList()}){
            if($sortHits->{$hit->getID()}){
                push @{$sortHits->{$hit->getID}},$hit;
            }
            else{
                $sortHits->{$hit->getID()} = [$hit];
            }
        }

	# get Annotation for hits
        foreach my $hitID(keys(%$sortHits)){

            # get hash-ref of annotation attribtues
            my $h = $dbi->getAnnotation($hitID);

	    ### HAS TO BE CHANGED ASAP!! BAD BAD BAD HARDCODED BS!
            if($dbConfig->getDatabaseFormat() eq "blast"){
                if($h->{'annotation'}){
                    $h->{'description'} = $h->{'annotation'};
                    delete $h->{'annotation'};
                }
            }
	    elsif($dbConfig->getDatabaseFormat() eq 'pfam'){
	    }
            else{
                $self->{'logger'}->LogError("Annotator::addAnnotation() - Unknown database format ".$dbConfig->getDatabaseFormat()."!");
            }

	    my $class = "GePan::Annotation::".ucfirst($dbConfig->getDatabaseFormat());
	    eval{
		_setHitAnnotation($class,$h,$sortHits,$hitID);
	    };
	    $self->{'logger'}->LogError("Annotator::addAnnotation() - $@") if($@);
        }
    }
}


=head2 B<_setHitAnnotation(class,params,hits,id)>

Initializes GePan::Annotation object and sets it to all hits. 

=cut

sub _setHitAnnotation{
    my ($class,$params,$hits,$id) = @_;

    my $module = $class;
    $module=~s/::/\//g;
    require "$module.pm";

    my $annotation = $class->new();
    $annotation->setParams($params);
    foreach(@{$hits->{$id}}){
        $_->setAnnotation($annotation);
    }
}


=head2 B<_getTransferredHits()>

Creates a GePan::Collection::Hit object of all transferred hits found for sequences.

=cut

sub _getTransferredHits{
    my ($self,$hitCollection,$transferredTools) = @_;

    # get hits of each tool
    while(my $config = $transferredTools->getNextElement()){

	# create path to output files of tool
	my $filePath = $self->{'work_dir'}."/tools/".$config->getID();
	$filePath=~s/\/\//\//g;
	next unless (-d $filePath);
	$self->{'logger'}->LogError("Annotator::getTransferredHit - No directory found for result files of tool ".$config->getID()) unless (-d $filePath);

	# get files name
	opendir(DIR,$filePath) or $self->{'logger'}->LogError("Annotator::getTransferredHit - Failed to open directory $filePath for reading.");
	my @files = grep{(-f "$filePath/$_")&&($_=~/^.*\.out.*$/)} readdir(DIR);
	closedir(DIR);
	$self->{'logger'}->LogError("Annotator::getTransferredHit - No result files found for tool ".$config->getID()) if (scalar(@files)<1);	

	my $class = $config->getParser();

	# go through all files of tool, parse them and add hits of sequences to collection
	foreach(@files){
	    my $parserParams = {file=>"$filePath/$_",
				database=>$self->{'database_collection'}->getDatabaseByFileName($_),
				logger=>$self->{'logger'}};
	    eval{
		$self->_readToolFile($class,$parserParams,$hitCollection);
	    };
	    $self->{'logger'}->LogError("Annotator::getTransferredHit - $@") if($@);
	}
    }

}


=head2 B<_readToolFile($class,$parserParams,GePan::Collection::Hit)>

Reads tool result file.

=cut

sub _readToolFile{
    my ($self,$class,$parserParams,$hitCollection) = @_;
   
    my $module = $class;
    $module=~s/::/\//g;
    require "$module.pm";

    my $parser = $class->new();
    $parser->setParams($parserParams);
    $parser->parseFile();

    while(my $hit = $parser->getCollection()->getNextElement()){
        if($self->{'sequences'}->getElementByID($hit->getQueryName())){
	    $hitCollection->addElement($hit);
	}
    }
}



=head2 B<_getFunctionalHits()>

Creates a GePan::Collection::Hit object of all functional hits found for sequences.

=cut

sub _getFunctionalHits{
    my ($self,$hitCollection,$functionalTools) = @_;

    # get hits of each tool
    while(my $config = $functionalTools->getNextElement()){

        # read directory of result files of tool
        my $filePath = $self->{'work_dir'}."/tools/".$config->getID();
        $filePath=~s/\/\//\//g;
	next unless (-d $filePath);
        $self->{'logger'}->LogError("Annotator::getFunctionalHits - No directory found for result files of tool ".$config->getID()) unless (-d $filePath);

	# get file names
	opendir(DIR,$filePath) or $self->{'logger'}->LogError("Annotator::getFunctionalHit - Failed to open directory $filePath for reading.");
        my @files = grep{(-f "$filePath/$_")&&($_=~/^.*\.out.*$/)} readdir(DIR);
        closedir(DIR);
        $self->{'logger'}->LogError("Annotator::getFunctionalHit - No result files found for tool ".$config->getID()) if (scalar(@files)<1);

	# create tool parser
	my $class = $config->getParser();

	# go through all files of tool, parse them and add hits of sequences to collection
        foreach my $fileName (@files){

	    my $parserParams = {file=>"$filePath/$fileName",
                                database=>$self->{'database_collection'}->getDatabaseByFileName($fileName),
				logger=>$self->{'logger'}};
	    eval{
                $self->_readToolFile($class,$parserParams,$hitCollection);
            };
            $self->{'logger'}->LogError("Annotator::getFunctionalHits - $@ ") if($@);
        }
    }
}



=head1 GETTER & SETTER METHODS

=head2 B<setWorkDir(string)>

Sets the working directory of project.

=cut

sub setWorkDir{
    my ($self,$p) = @_;
    $self->{'work_dir'} = $p;
}

=head2 B<getWorkDir()>

Returns working directory of project.

=cut

sub getWorkDir{
    my $self=  shift;
    return $self->{'work_dir'};
}


=head2 B<setAnnotationTools(GePan::Collection::ToolConfig)>

Sets collection of tools used to annotate given sequence type

=cut

sub setTools{
    my ($self,$c) = @_;
    $self->{'annotation_tools'} = $c;
}


=head2 B<setAttributeTools(GePan::Collection::ToolConfig)>

Sets collection of tools used for attribute annotation of given sequence type

=cut

sub setAttributeTools{
    my ($self,$c) = @_;
    $self->{'attribute_tools'} = $c;
}


=head2 B<getSequences()>

Returns Collection::Sequence object.

=cut

sub getSequences{
    my $self = shift;
    return $self->{'sequences'};
}

=head2 B<getTools()>

Returns GePan::Collection::ToolConfig .

=cut

sub getTools{
    my $self = shift;
    return $self->{'tools'};
}


=head2 B<setLogger(GePan::Logger)>

Sets GePan::Logger object.

=cut

sub setLogger{
    my ($self,$l) = @_;
    $self->{'logger'} = $l;
}


=head2 B<setParams(ref)>

Sets all attributes of object by hash ref.

=cut

sub setParams{
    my ($self,$h) = @_;
    foreach(keys(%$h)){
        $self->{$_} = $h->{$_};
    }
}



=head1 INTERNAL METHODS

=head2 B<_load()>

Loads required classes for this package.

=cut

sub _load(){

    # load collection classes
    my $collectionDir = GEPAN_PATH."/GePan/Collection";
    opendir(DIR,$collectionDir);
    my @classes = grep{$_=~/.*\.pm/}readdir(DIR);
    closedir(DIR);
    foreach(@classes){
	my $class = $collectionDir."/$_";
	eval{_requireClass($class)};
	die $@ if $@;
    }


    # load hit classes
    my $hitDir = GEPAN_PATH."/GePan/Hit";
    opendir(DIR,$hitDir);
    @classes = grep{$_=~/.*\.pm/}readdir(DIR);
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







