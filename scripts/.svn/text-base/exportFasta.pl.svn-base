#!/opt/local/perl5122/bin/perl
use strict;
use Getopt::Std;
use GePan::Parser::Input::Fasta;
use GePan::Exporter::Fasta;
use GePan::Logger;
use GePan::Collection::Sequence;


=head1 NAME

    exportFasta.pl

=head1 DESCRIPTION

Script for parsing prediction-tool output files. Writes out a fasta file of all predicted sequences.

=head1 PARAMETER

p: string of parameters the parser needs

c: class of the tool parser 

s: sequence file the prediction was based on

t: output type, either protein, nucleotide or both separated by ','

o: output directory
=cut


eval{
    _main();
};
if($@){
    print $@;
}


sub _main{
    our %opts;
    getopts("p:c:s:t:o:",\%opts);

    my $paramString = $opts{'p'};
    my $class = $opts{'c'};
    my $outputTypes = $opts{'t'};
    my $parentFile = $opts{'s'};
    my $out = $opts{'o'};

    if(!$paramString||!$outputTypes||!$parentFile||!$out){
	die "Missing parameters for exportFasta.pl";
    }

    my $parentCollection = _createParentSequences($parentFile);

    my $params = _createParams($paramString);
    my $logger = GePan::Logger->new();
    $logger->setStatusLog("/state/partition1/gepan/gepan.log");
    $logger->setNoPrint(1);
    $params->{'logger'} = $logger;
     
    $params->{'parent_sequences'} = $parentCollection;

    my $collection = GePan::Collection::Sequence->new();
    eval{
	_runToolParser($class,$params,$collection);
    };
    if($@){
	die $@;
    }	 

    my $exporter = GePan::Exporter::Fasta->new();
    $exporter->setParams({output_directory=>$out,
			  logger=>$logger,
			  file=>'exporter.fas',
			  output_types=>$outputTypes,
			  collection=>$collection});
    $exporter->export();

    # dump all sequences to xml file
    _dumpSequences($collection,$out);


}



sub _dumpSequences{
    my ($collection,$out) = @_;

    open(OUT,">$out/collection.xml") or die "Failed to open file $out/collection.xml file for writing.";
    print OUT "<collection>\n";
    my $dump = Data::Dumper->new([$collection],[qw($collection)])->Purity(1)->Dump();
    print OUT $dump;
    print OUT "</collection>";
    close(OUT);
}



=head2 B<_createParams(string)>

Creates Parameter hash from given parameter string.

=cut

sub _createParams{
    my $string = shift;
    
    my @atts = split(";",$string);
    
    my $params = {};
    foreach(@atts){
	my @split = split("=",$_);
	$params->{$split[0]} = $split[1];
    }

    return $params;
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


=head2 B<_createParentSequences(file)>

Reads in parent sequences (usually contig fasta file) and creates GePan::Collection::Sequence object from it.

=cut

sub _createParentSequences{
    my $inputFile = shift;

    my $logger = GePan::Logger->new();
    $logger->setNoPrint(1);

    my $parser = GePan::Parser::Input::Fasta->new();
    $parser->setParams({type=>'contig',
			file=>$inputFile,
			logger=>$logger});

    $parser->parseFile();
    return $parser->getCollection();
}



