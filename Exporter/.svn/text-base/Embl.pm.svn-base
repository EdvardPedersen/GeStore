package GePan::Exporter::Embl;
use base qw(GePan::Exporter);
use GePan::Collection::Sequence;
use strict;
use Data::Dumper;

=head1 NAME

GePan::Exporter::Embl

=head1 DESCRIPTION

Simple embl-exporter of annotation run. Each contig and predicted sequences are written to a single embl file named CONTIG_NAME.embl.

=head1 ATTRIBUTES

parent_collection: GePan::Collection::Sequence object of contigs/reads/

collection: GePan::Collection::Sequenc object of predicted CDS and other features.

strict: if set just qualifier following the embl-definition are used. This results in all annotation and hit information being concatenated and put into qualifier "/note". Without strict new qualifiers named after the software used are introduced into the output file.

=head1 METHODS

=head2 B<export()>

Exports given sequence(s) to a fasta file of given name to given directory.

Additionally sets attribute 'tmp_file' to all sequence objects.

=cut

sub export{
    my $self = shift;

    $self->{'logger'}->LogError("GePan::Exporter::Embl::export() - No output directory set.") unless $self->{'output_directory'};
    $self->{'logger'}->LogError("GePan::Exporter::Embl::export() - No collection of CDS/features given.") unless $self->{'collection'};    
    $self->{'logger'}->LogError("GePan::Exporter::Embl::export() - No collection of contig sequences given.") unless $self->{'parent_collection'};    

    while(my $contig = $self->{'parent_collection'}->getNextElement()){
	
	my $seqs = GePan::Collection::Sequence->new();
	my $contigGeneNames1 = $contig->getID()."_orf";
	my $contigGeneNames2 = $contig->getID()."_gene";
	while(my $seq = $self->{'collection'}->getNextElement()){
	    if(($seq->getID()=~m/$contigGeneNames1/)||($seq->getID()=~m/$contigGeneNames2/)){
		$seqs->addElement($seq);
	    }
	}	
    
	my $file = $self->{'output_directory'}."/".$contig->getID().".embl";
	$file=~s/\/\//\//g;
	open(OUT,">$file") or $self->{'logger'}->LogError("GePan::Exporter::Embl::export() - Failed to open result embl file $file for writing.");
	$self->_printEmblHeader($contig,*OUT);
	while(my $seq = $seqs->getNextElement()){
	    $self->_printSequence($seq,*OUT);
	}	
	# print footer with sequence
	$self->_printFooter($contig->getSequence(),*OUT);
    }




}


=head1 INTERNAL METHODS


=haed2 B<_printFooter(sequence,filehandle)>

Takes a sequence and creates the footer of an embl file from it.

=cut

sub _printFooter{
    my ($self,$sequence,$fh) = @_;

    my $bp = length($sequence);
    my $copy = $sequence;
    my $a = $copy=~s/a//ig;
    my $t = $copy=~s/t//ig;
    my $c = $copy=~s/c//ig;
    my $g = $copy=~s/g//ig;
    my $other = length($copy);
    print $fh "SQ   Sequence $bp BP; $a A; $c C; $g G; $t T; $other other;\n";


    my @lines = ();
    my $rest = '';
    my $word = '';
    my $l = '';
    my @c = split('',$sequence);
    foreach my $ca(@c){
	if(length($word)<10){
	    $word.=$ca;
        }
        else{
	    if(length($l)>61){
		push @lines,$l;
                $l = " $word";
                $word = $ca;
            }
            else{
		$l.=" $word";
                $word = $ca;
            }
        }
    }
    if(length($l)<61){
        push @lines,"$l $word";
    }
    else{
        push @lines,$l;
        push @lines, " $word";
    }

    # print lines
    my $max = (scalar(@lines)-1)*60;
    my $last = pop(@lines);
    for(my $i = 1;$i<=scalar(@lines);$i++){
        print $fh "    ".$lines[$i-1];
        print $fh " " x ((length($max)+3)-(length($i*60)));
        print $fh ($i*60)."\n";
    }
    if(length($last)>60){
        print $fh "    $last";
        print $fh " "x 3;
        print $fh ($max+60)."\n";
    }
    else{
        print $fh "    $last";
        print $fh " "x(73-(4+length($last))+(length($max)-length($max+60)));
        $last=~s/ //g;
        print $fh ($max+length($last))."\n";
    }
}


=head2 B<_printSequence(GePan::Sequence,filehandle)>

Prints a GePan::Sequence object plus annotation to embl file.

=cut

sub _printSequence{
    my ($self,$seq,$fh) = @_;

    # print CDS line
    my $cdsString = "FT\t".uc($seq->getType())."\t";
    if($seq->getComplement()){
	$cdsString.="complement("._getMin($seq->getStart(),$seq->getStop()).".."._getMax($seq->getStart(),$seq->getStop()).")";
    }
    else{
	$cdsString.=_getMin($seq->getStop(),$seq->getStart()).".."._getMax($seq->getStart(),$seq->getStop());
    }
    $self->_printLine($cdsString,$fh);
    
    # print locus_tag
    my $locus = "FT\t/locus_tag=\"".$seq->getID()."\"";
    $self->_printLine($locus,$fh);

    # sequence annotation
    my $anno = $seq->getAnnotation();
    if($self->{'strict'}){
	$self->_printStrictAnnotation($anno,$fh);
    }   
    else{
	$self->_printNonStrictAnnotation($anno,$fh);
    }


}



=head2 B<_printNonStrictAnnotation(GePan::SequenceAnnotation,filehandle)>

Print annotation information in embl format using one qualifier for each value of each tool.

=cut

sub _printNonStrictAnnotation{
    my ($self,$annotation,$fh) = @_;

    # get product tag
    my $note = "";
    if((!($annotation->getTransferredAnnotation())||($annotation->getTransferredAnnotation()->getID() eq 'empty'))&&(!($annotation->getFunctionalAnnotation())||($annotation->getFunctionalAnnotation()->getID() eq 'empty'))){
	my $product="/product=\"Putative uncharacterized protein\"";
	$product="FT\t".$product;
	$self->_printLine($product,$fh);
	$note = "/note=\"No similarity to any database sequence found\"";
    }
   
    # print transferred hit information
    if(($annotation->getTransferredAnnotation())&&($annotation->getTransferredAnnotation()->getID() ne 'empty')){
        my $seqAnno = $annotation->getTransferredAnnotation()->getAnnotation();
        my $transHit = $annotation->getTransferredAnnotation();
	my $product="/product=\"".$seqAnno->getDescription();
        $product=~s/\t/ - /g;
        $product="FT\t".$product."\"";
        $self->_printLine($product,$fh);

	# print tool of transferred hit
        my $transString = "";
        $transString = "/transferred_hit=\"".$transHit->{'tool'};
        $transString=~s/\t/ - /g;
        $transString="FT\t".$transString."\"";
        $self->_printLine($transString,$fh);

	# print hit values
        my $hitString = "";
        my $atts = $transHit->_getAttributes();
        foreach(@$atts){
            next unless $transHit->{$_};
            next unless ($_ ne "tool");
            $hitString.="$_: ".$transHit->{$_}."; ";
        }
	$hitString = "/".$transHit->{'tool'}."_hit=\"".$hitString;
	$hitString=~s/\t/ - /g;
	$hitString="FT\t".$hitString."\"";
        $self->_printLine($hitString,$fh);

	# print annotation of transferred hit
	my $annoString = "";
	my $transAnno = $transHit->getAnnotation();
	$atts = $transAnno->_getAttributes();
	foreach(@$atts){
            next unless $transAnno->{$_};
            $annoString.="$_: ".$transAnno->{$_}."; ";
        }
	$annoString="/".$transHit->{'tool'}."_annotation=\"".$annoString;
	$annoString=~s/\t/ - /g;
        $annoString="FT\t".$annoString."\"";
        $self->_printLine($annoString,$fh);
    }
    else{
        my $transString = "FT\t/transferredHit=\"No transferred hit found by any tool\"";
        $self->_printLine($transString,$fh);
    }

 
    # print functional hit information
    if(($annotation->getFunctionalAnnotation())&&($annotation->getFunctionalAnnotation()->getID() ne 'empty')){
	my $seqAnno = $annotation->getFunctionalAnnotation()->getAnnotation();
        my $funcHit = $annotation->getFunctionalAnnotation();
	my $atts = $funcHit->_getAttributes();
	
	# if there is no transferred annotation set CDS
	# product to pfam hit annotation
	if(($annotation->getTransferredAnnotation())&&($annotation->getTransferredAnnotation()->getID() eq 'empty')){
	    my $product="/product=\"".$seqAnno->getDBComment();
	    $product=~s/\t/ - /g;
	    $product="FT\t".$product."\"";
	    $self->_printLine($product,$fh);
	}

	# print tool name of functional hit
	my $funcString = "/functional_hit=\" ".$funcHit->{'tool'};
	$funcString=~s/\t/ - /g;
        $funcString="FT\t".$funcString."\"";
        $self->_printLine($funcString,$fh);

	# print hit values
	my $hitString = "/".$funcHit->{'tool'}."_hit=\"";
	foreach(@$atts){
            next unless $funcHit->{$_};
	    next unless ($_ ne "tool");
            $hitString.="$_: ".$funcHit->{$_}."; ";
        }
        $hitString=~s/\t/ - /g;
        $hitString="FT\t".$hitString."\"";
        $self->_printLine($hitString,$fh);

	# print functional annotation values
	my $annoString = "/".$funcHit->{'tool'}."_annotation=\" ";
	my $funcAnno = $funcHit->getAnnotation();
	$atts = $funcAnno->_getAttributes();
	foreach(@$atts){
	    next unless $funcHit->{$_};
	    $annoString.="$_: ".$funcAnno->{$_}."; ";
        }
        $annoString=~s/\t/ - /g;
        $annoString="FT\t".$annoString."\"";
        $self->_printLine($annoString,$fh);
    }
    else{
	my $funcString = "FT\t/functional_hit=\"No functional hit found by any tool\"";
        $self->_printLine($funcString,$fh);
    }

    while(my $aHit = $annotation->getAttributeCollection()->getNextElement()){
	my $atts = $aHit->_getAttributes();
	my $toolString = "/".lc($aHit->getToolName())."=\"";
	foreach(@$atts){
	    next if ($_ eq "tool");
	    $toolString.= "$_: ".$aHit->{$_}."; ";    
	}
	$toolString=~s/\t/ - /g;
	$toolString = "FT\t$toolString\"";
	$self->_printLine($toolString,$fh);
    }
}

=head2 B<_printStrictAnnotation(GePan::SequenceAnnotation,filehandle)>

Print annotation information following the embl definition, i.e. all information about used tools is concatenated and written in qualifier '/note'.

=cut

sub _printStrictAnnotation{
    my ($self,$annotation,$fh) = @_;

    # get product tag
    my $note = "";
    if(($annotation->getTransferredAnnotation()->getID() eq 'empty')&&($annotation->getFunctionalAnnotation()->getID() eq 'empty')){
	my $product="/product=\"Putative uncharacterized protein\"";
	$product="FT\t".$product."\"";
	$self->_printLine($product,$fh);
	$note = "/note=\"No similarity to any database sequence found\"";
    }
    elsif(($annotation->getTransferredAnnotation()->getID() eq 'empty')&&($annotation->getFunctionalAnnotation()->getID() ne 'empty')){
	my $seqAnno = $annotation->getFunctionalAnnotation()->getAnnotation();
	my $funcHit = $annotation->getFunctionalAnnotation();
	if($funcHit->getDB()->getID() eq 'pfam-b'){
	    my $product="/product=\"Putative uncharacterized protein\"";
	    $product=~s/\t/ - /g;
	    $product="FT\t".$product;
	    $self->_printLine($product,$fh);
	}
	else{
	    my $product="/product=\"".$seqAnno->getDBComment();
	    $product=~s/\t/ - /g;
	    $product="FT\t".$product."\"";
	    $self->_printLine($product,$fh);
	}
	$note = "/note=\"Best functional hit to ".$annotation->getFunctionalAnnotation()->getID()."; Accession num: ".$annotation->getFunctionalAnnotation()->getAccessionNumber()."; Database: ".$annotation->getFunctionalAnnotation()->getDB()->getID()."; Evalue = ".$annotation->getFunctionalAnnotation()->getEValue()."; Score = ".$annotation->getFunctionalAnnotation()->getScore();
	$note.=" Significance: 1" if ($annotation->getFunctionalAnnotation()->getSignificance());
	$note.=" Significance: 1" unless ($annotation->getFunctionalAnnotation()->getSignificance());
    }
    elsif(($annotation->getTransferredAnnotation()->getID() ne 'empty')&&($annotation->getFunctionalAnnotation()->getID() eq 'empty')){
	my $seqAnno = $annotation->getTransferredAnnotation()->getAnnotation();
	my $hit = $annotation->getTransferredAnnotation();
	my $product="/product=\"".$seqAnno->getDescription();
	$product=~s/\t/ - /g;
	$product="FT\t".$product."\"";
	$self->_printLine($product,$fh);
	$note = "/note=\"Best transferred hit to ".$hit->getID().";";
	my $hitParams = $hit->_getAttributes();
	foreach(@$hitParams){
	    next unless $hit->{$_};
	    $note.=" $_: ".$hit->{$_}."; ";
	}
    }
    elsif(($annotation->getTransferredAnnotation()->getID() ne 'empty')&&($annotation->getFunctionalAnnotation()->getID() ne 'empty')){
	my $transAnno = $annotation->getTransferredAnnotation()->getAnnotation();
	my $funcAnno = $annotation->getFunctionalAnnotation()->getAnnotation();
        my $transHit = $annotation->getTransferredAnnotation();
	my $funcHit = $annotation->getFunctionalAnnotation();
        my $product="/product=\"".$transAnno->getDescription();
	$product=~s/\t/ - /g;
	$product="FT\t".$product."\"";
	$self->_printLine($product,$fh);
        $note = "/note=\"Best transferred hit to ".$transHit->getID()."; ";
	my $hitParams = $transHit->_getAttributes();
        foreach(@$hitParams){
	    next unless $transHit->{$_};
            $note.="$_: ".$transHit->{$_}."; ";
        }
	$note .= " Best functional hit to ".$funcHit->getID();
        $hitParams = $funcHit->_getAttributes();
        foreach(@$hitParams){
            next unless $funcHit->{$_};
            $note.=" $_: ".$funcHit->{$_}."; ";
        }
    }
    while(my $aHit = $annotation->getAttributeCollection()->getNextElement()){
	my $atts = $aHit->_getAttributes();
	$note.=" ; Hit of ".$aHit->getToolName()." - ";
	foreach(@$atts){
	    $note.= " $_: ".$aHit->{$_}."; ";    
	}
    }
    $note=~s/\t/ - /g;
    $note = "FT\t$note\"";
    $self->_printLine($note,$fh);
}



=head2 B<_printEmblHeader(GePan::Sequence,$filehandle)>

Print the header of an embl file.

=cut

sub _printEmblHeader{
    my ($self,$contig,$fh) = @_;
    print $fh "ID   Total      standard; DNA; UNC;  ".$contig->getLength()." BP.\n";
    print $fh "FH   Key             Location/Qualifiers\n";
    print $fh "FH\n";
    print $fh "FT   source          1..".$contig->getLength()."\n";
    print $fh "FT                   /origid=\"".$contig->getID()."\"\n"; 
}


=head2 B<_printLine(string,filehandle)>

Prints one embl line reagarding the embl format. Given string is split at whitespace. First word will be written into first 1-5 columns, second to 6-20, rest columns 22-80.

=cut

sub _printLine{
    my ($self,$string,$fh) = @_;



    my @split = split(/\t/,$string);
    
    if($string=~/^(.*)\t(.*)\t(.*)\t(.*)$/){
	    print STDOUT "$1\n$2\n$3";
	    die;
    }

    my $s = "";

    # get first 5 columns
    $self->{'logger'}->LogError("GePan::Exporter::Embl::_printLine() - Length of first element in embl string split longer than 5 characters: ".$split[0]) unless (length($split[0])<=5);    
    $s.=$split[0];
    while(length($s)<5){
	$s.=" ";
    }

    # get column 6-21
    if(scalar(@split==3)){
	$s.=$split[1];
    }
    while(length($s)<21){
	$s.=" ";
    }

    # print rest to columns 22-80
    # check if given text is longer than 58 characters
    my $rest;
    if(scalar(@split)==2){
	$rest = $split[1];
    }
    elsif(scalar(@split)==3){
	$rest = $split[2];
    }
    else{
	$self->{'logger'}->LogError("GePan::Exporter::Empl::_printLine() - Odd number of splits in string:\n".Dumper @split);
    }

    my @split2 = split(" ",$rest);
    if(length($rest)<=58){
	$s.=$rest."\n";
	print $fh $s;
    }
    else{
	my @sp2 = split(" ",$rest);
	for(my $a = 0;$a<scalar(@sp2);$a++){
	    # length of text strings of qualifiers
	    if(length($s." ".$sp2[$a])<80){
		if($a==0){
		    $s.=$sp2[$a];
		}
		else{
		    $s.=" ".$sp2[$a];
		}
	        print $fh $s."\n" unless ($a+1 != scalar(@split2));
	    }
	    else{
		print $fh $s."\n";
		$s = $split[0];
		# distance from beginning of second and following lines of a split string
		while(length($s)<21){
		    $s.=" ";
	        }   
	        $s .= $sp2[$a];
		if($a==(scalar(@sp2)-1)){
		    print $fh $s."\n";
		}
	    }
	}
    }
}

=head1 GETTER & SETTER METHODS

=head2 B<setCollection(GePan::Collection::Sequence)>

Sets the sequence-object(s) of the exporter.

=cut

sub setCollection{
    my ($self,$seqs) = @_;
    $self->{'collection'} = $seqs;
}

=head2 B<getCollection()>

Returns the GePan::Collection::Sequence object.

=cut

sub getCollection{
    my $self = shift;
    return $self->{'collection'};
}

=head2 B<setCollection(GePan::Collection::Sequence)>

Sets the sequence-object(s) of contig sequences for the exporter.

=cut

sub setParentCollection{
    my ($self,$seqs) = @_;
    $self->{'collection'} = $seqs;
}

=head2 B<getParentCollection()>

Returns the GePan::Collection::Sequence object of parent sequences.

=cut

sub getParentCollection{
    my $self = shift;
    return $self->{'collection'};
}

=head2 B<setStrict()>

If set to '1' just qualifiers allowed in the embl definition are used.

=cut

sub setStrict{
    my ($self,$v) = @_;
    $self->{'strict'} = $v;
}


=head1 INTERNAL METHODS

=head2 B<_getMin(int,int)>

Returns smaller of given values;

=cut
sub _getMin{
    my ($a,$b) = @_;
    if($a<$b){
	return $a;
    }
    else{
	return $b;
    }
}

=head2 B<_getMax(int,int)>

Returns bigger of given values.

=cut

sub _getMax{
    my ($a,$b) = @_;
    if($a>$b){
	return $a;
    }
    else{
	return $b;
    }
}





1;
