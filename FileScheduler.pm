package GePan::FileScheduler;

use strict;
use Data::Dumper;
use GePan::Exporter::Fasta;
use GePan::Collection::Sequence;
use GePan::Logger;

=head1 NAME

GePan::FileScheduler

=head1 DESCRIPTION

Package for creating multiple fasta files with at most 
given number of sequences per file. 

=cut

=head1 ATTRIBUTES

max = maximum number of fasta files to create (number of CPUs used)

collection = GePan::Collection::Sequence object containing sequences.

output_directory = directory the fasta files are written too

file_names = array-ref of outomatically generated fasta file names (created by FileScheduler).

logger: GePan::Logger object

=head1 CONSTRUCTOR

=head2 B<new()>

Creates an empty FileScheduler object.

=cut

sub new{
    my ($class) = @_;
    my $self = {max=>'',
		collection=>'',
		output_directory=>'',
		file_names=>''};
    return (bless($self,$class));
}

=head1 METHODS

=head2 B<createFiles()>

Creates max number of sequence files from sequences in given sequence_object.

=cut

sub createFiles{
    my $self = shift;
    
    $self->{'logger'}->LogError("FileScheduler::createFiles() - No maximum number of sequence-file(s) given.") unless $self->{'max'};
    $self->{'logger'}->LogError("FileScheduler::createFiles() - No GePan::Collection::Sequence object given.") unless $self->{'collection'};
    $self->{'logger'}->LogError("FileScheduler::createFiles() - No output-directory given.") unless $self->{'output_directory'};

    my $collection = $self->{'collection'};
    my $files = [];

    my $seqs = _sort($collection,$self->{'max'}); 

    my $path = $self->{'output_directory'}."/";
    $path=~s/\/\//\//g;
    
    for(my $i = 0;$i<scalar(@$seqs);$i++){
	my $tmp = {};
	my $type;
	my $splitCollection = GePan::Collection::Sequence->new();
	while(my $seq = $seqs->[$i]->getNextElement()){
	    $self->{'logger'}->LogError("FileScheduler::createFiles() - Unknown sequence of name ".$seq->getID()) unless ($collection->getElementByID($seq->getID));
	    if($type && ($seq->getType() ne $type)){
		$self->{'logger'}->LogWarning("Sequences of different types are included in file.");
	    }
	    if(!$type){
		$type = $seq->getType();
	    }
	    $splitCollection->addElement($seq);
	}
	
	my $name = _getFileName($self->{'file'},$path);
	my $fasta = GePan::Exporter::Fasta->new();
	my $input_type = $self->{'collection'}->getList()->[0]->getSequenceType();
	$fasta->setParams({output_directory=>$path,
			   file=>$name,
			   output_types=>$input_type,
			   collection=>$splitCollection});
	$fasta->export();
	push @$files,$name;
    }
    $self->{'file_names'} = $files;
}


=head1 GETTER & SETTER METHODS

=head2 B<setParams(hash-ref)>

Sets all parameter by hash of form
{ parameter_name => parameter_value }

=cut

sub setParams{
    my ($self,$params) = @_;
    foreach(keys(%$params)){
        $self->{$_} = $params->{$_};
    }
}


=head2 B<getNextSequenceFile()>

Returns the path/name of the next sequence file in list of 
all files created by createFile().
Returns 0 if all files have been returned.
Starts at the beginning of the list if called again.

=cut

sub getNextSequenceFile{
    my $self = shift;
    if($self->{'fileCount'}){
	if(($self->{'fileCount'}+1)<scalar(@{$self->{'splitFiles'}})){
	    $self->{'fileCount'} = $self->{'fileCount'}+1;
	    return @{$self->{'splitFiles'}}[$self->{'fileCount'}];
	}
	else{
	    $self->{'fileCount'} = undef;
	    return 0;
	}
    }
    else{
	$self->{'fileCount'} = 0;	
	return @{$self->{'splitFiles'}}[$self->{'fileCount'}];
    }
}
    

=head2 returnFileNames

Returns the names of all created fasta files

=cut

sub returnFileNames{
    my $self = shift;
    $self->{'logger'}->LogError("FileScheduler::returnFileNames() - No known result files.") unless $self->{'file_names'};
    return $self->{'file_names'};
}


=head1 INTERNAL METHODS

=head2 B<_sort(hash-ref,int)>

Method to sort all sequences of the sequence_object to 'int' number of files.

Returns array-ref with maximal max number of array-ref of sequence names. Sequences sorted by length and distributed to files so that each file has approximately same length.

Implements a basic sorting algorithm:

1. Take 'max' number of longest sequences and add one to each of 'max' number files

2. Take 'max' number of shortest sequences and add one to each of 'max' number of files

3. Start again at step 1

=cut


sub _sort{
    my ($collection,$number) = @_;

    # initialize the resulting array of collection objects
    my @result = ();

    # create array with keys longest sequence to shortest sequence
    my $tmp = [];
    foreach(sort{($collection->getElementByID($b)->getLength())<=>($collection->getElementByID($a)->getLength())} (map($_->{'id'},@{$collection->getList()}))){
        push @$tmp,$_;
    }


    # Switch that tells if element from front or back should be added
    my $switch = 0;

    # back_count index
    my $bCount = -1;

    # front_count index
    my $fCount = -1;

    # number of sequences in total
    my $length = @$tmp;


    # To distribute $length elements to $e different
    # files algorithm works like this:
    # 1. Take first $e elements of all sorted sequences 
    #    and push them into $e result-elements
    # 2. Take last $e elements of all sorted sequences
    #    and push them into $e result-elements
    # 3. Start again with 1 until all sequences are 
    #    done.
    for(my $i = 0;$i<scalar(@{$collection->getList()});$i++){
        my $a = $i%$number;
	# Switch from front to back index
        if(!($i%$number)){
            $switch = $switch?0:1;
	    # set number of runs, either fron-run or back-run
            if(!$switch){
                $bCount++;
            }
            else{
                $fCount++;
            }
        }

	# If switch then take from front (longer sequences)
	# else from back (shorter sequences)
	my $ind;
        if($switch){
	    # index of which longer sequence to push
	    # to result at index $a
            $ind = ($fCount*$number)+$a;
	}
	else{
	    # index of which shorter sequence to push
            # to result at index $a
	    $ind = $length-(($bCount*$number)+$a)-1;
	}

	# if there is already a sequence-collection at result[$a] just add the sequence-object
	# otherwise create a collection and add the sequence-object
        if(ref $result[$a]){
	    my $c = $result[$a];
	    $c->addElement($collection->getElementByID($tmp->[$ind]));
        }
        else{
	    my $c = GePan::Collection::Sequence->new();
	    $c->addElement($collection->getElementByID($tmp->[$ind]));
            $result[$a] = $c;
        }
    }

    return \@result;
}

=head2 B<_getFileName()>

Returns unique file-name for fasta file dependend on sequence type

File names are: filename.XXX where XXX = count 

=cut

sub _getFileName{
    my ($input,$path) = @_;

    my $counter = 1;

    # get name of input file
    my @split = split("/",$input);
    my $file = $split[-1];

    while(-f ($path."/".$file.".".$counter)){
	$counter++;
    } 
    return ($file.".".$counter);
}

=head2 B<setLogger(GePan::Logger)>

Sets GePan::Logger object.

=cut

sub setLogger{
    my ($self,$l) = @_;
    $self->{'logger'} = $l;
}

### No sort function skips sorting. Not done yet
#sub _nosort{
#    my $collection = shift;
#    my @result = ();
#    foreach (@{$collection->getList()}){push @results, $_;}
#    return \@result; 

#}

1;
