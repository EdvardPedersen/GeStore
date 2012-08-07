#!/usr/bin/perl

=head1 NAME

runScheduler.pl

=head1 DESCRIPTION

Creates number of fasta files of (more or less) same size from input fasta-files.

Uses GePan::Filescheduler for sorting and writing out files.

=head1 PARAMETER

i: input file 

o: output directory

n: maximum number of fasta-files created (usually number of cpus)

=cut

use strict;
use Data::Dumper;
use Getopt::Std;
use GePan::Parser::Input::Fasta;
use GePan::FileScheduler;
use GePan::Sequence::Type::Contig;
use GePan::Collection::Sequence;

eval{
    _main();
};

if($@){
    print $@; 
}



sub _main{
    our %opts;
    getopts("i:o:n:s:",\%opts);

    my $in = $opts{'i'};
    my $out = $opts{'o'};
    my $max = $opts{'n'};
    my $sort = $opts{'s'};

    if(!$in||!$max||!$out){
	_usage();
    }

    my $logger = GePan::Logger->new();
    $logger->setStatusLog("$in/gepan.log");
    $logger->setNoPrint(1);

    my $reader = GePan::Parser::Input::Fasta->new();
    $reader->setParams({file=>$in,
                               logger=>$logger,
                               type=>'cds'});
    $reader->parseFile();

    # prepare FileScheduler
    my $scheduler = GePan::FileScheduler->new();
    $scheduler->setParams({max=>$max,
			   collection=>$reader->getCollection(),
			   logger=>$logger,
			   type=>'cds',
			   sorting=>$sort,
			   file=>$in,
			   output_directory=>$out});
    $scheduler->createFiles();
}

sub _usage{
    print STDOUT "\n\nScript preprocesses fasta files and distributes them to a given number of files.\n";
    print STDOUT "Uses GePan::Filescheduler for sorting sequences.\n";
    print STDOUT "Parameter:\n";
    print STDOUT "i : input file.\n";
    print STDOUT "o : directory for output files\n";
    print STDOUT "n : number of result files (maximum)\n\n";
    print STDOUT "s : Perform sort optimization\n";
    exit;
}

