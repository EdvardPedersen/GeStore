package GePan::Config;

=head1 NAME

GePan::Common

=head1 DESCRIPTION

Package for storing needed paths, e.g. path to blast, fasta etc

=head1 EXPORTED CONSTANTS

BLAST_PATH : path to blastall

FASTA_PATH : path to executables of the fasta package, e.g. fasta35,fastx ... 

PFAM_PATH : path to hmmsearch

MGA_PATh : path to MetaGeneAnnotator

GLIMMER3_PATH : path to glimmer3

DATABASE_PATH : path to database directory (with sub-directories pfam and uniprot)

PRIAM_PATH : path to PRIAM search tool

PRIAM_RELEASE_PATH : path to the current release of PRIAM used (Directory)

PRIAM_BLAST_PATH : path to BLAST-tools needed by Priam (Directory)

=cut
use strict;
use base qw(Exporter);

use constant BLAST_PATH=>'/share/apps/gepan/share/blast/blastall';
use constant PFAM_PATH=>'/opt/local/hmmer30rc2/binaries/hmmscan';
use constant FASTA_PATH=>'/opt/bio/fasta/fasta36';
use constant MGA_PATH=>'/share/apps/gepan/share/mga/mga_linux_ia64';
use constant GLIMMER3_PATH=>'/opt/bio/glimmer/scripts';
use constant SIGNALP_PATH=>'/share/apps/gepan/share/signalp/signalp-3.0/signalp';
use constant DATABASE_PATH=>'/share/apps/gepan/bio_databases';
use constant PERL_PATH=>'/opt/local/perl5122/bin/perl';
use constant GEPAN_PATH=>'/home/emr023'; #BAD
use constant NODE_LOCAL_PATH=>'/state/partition1';
use constant PRIAM_PATH=>'/opt/local/PRIAM_search.jar';
use constant PRIAM_RELEASE_PATH=>'/share/apps/gepan/bio_databases/priam/PRIAM_OCT11';
use constant PRIAM_BLAST_PATH=>'/opt/bio/ncbi/bin';
use constant MEGAN_PATH=>'/home/emr023/MEGANTEST/megan/MEGANTEST'; #BAD
use constant KRONA_PATH=>'/opt/local/kronatools20/bin';
use constant GESTORE_PATH=>'/home/epe005/DiffDBMR/diffdb.jar'; #BAD
use constant BLAST2XML_PATH=>'/opt/local/python27/bin/python /home/epe005/DiffDBMR/flatfileToXml.py'; #BAD

our @EXPORT_OK=qw(BLAST_PATH FASTA_PATH PFAM_PATH MGA_PATH GLIMMER3_PATH DATABASE_PATH SIGNALP_PATH PERL_PATH GEPAN_PATH NODE_LOCAL_PATH PRIAM_PATH PRIAM_BLAST_PATH PRIAM_RELEASE_PATH MEGAN_PATH KRONA_PATH GESTORE_PATH BLAST2XML_PATH);

1;

