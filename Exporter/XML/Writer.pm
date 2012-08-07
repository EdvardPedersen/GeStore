package GePan::Exporter::XML::Writer;

use strict;

=head1 NAME

    GePan::Exporter::XML::Writer

=head1 DESCRIPTION

Main class for GePan::Exporter::XML::Writer. 

=head1 ATTRIBUTES

fh: References to opened filehandle of the output file.

depth: Depth of the tag. Defines how many tabs are included for base tags.

logger: GePan::Logger object

class: Class of object, e.g. SequenceAnnotation

=head1 METHODS

=head2 B<start()>

Prints opening '<tag>' (objects class attribute) with depth leading tabs.

=cut

sub start{
    my $self = shift;
    $self->{'logger'}->LogError("Exporter::XML::Writer::start() - No filehandle given.") unless ref($self->{'fh'});
    my $fh = $self->{'fh'};

    $self->{'logger'}->LogError("Exporter::XML::Writer::start() - No depth given.") unless exists($self->{'depth'});
    my $depth = $self->_getTabs($self->getDepth());

    $self->{'logger'}->LogError("Exporter::XML::Writer::start() - No class set for element.") unless $self->getClass();
    my $class = $self->getClass();    

    print $fh "$depth<$class>\n";
}

=head2 B<stop()>

Prints closing '</tag>' (objects class attribute) with depth leading tabs.

=cut

sub stop{
    my $self = shift;
    $self->{'logger'}->LogError("Exporter::XML::Writer::stop() - No filehandle given to XML::Writer.") unless ref($self->{'fh'});
    my $fh = $self->{'fh'};

    $self->{'logger'}->LogError("Exporter::XML::Writer::stop() - No depth given.") unless exists($self->{'depth'});
    my $depth = $self->_getTabs($self->getDepth());
    
    $self->{'logger'}->LogError("Exporter::XML::Writer::stop() - No class set for element.") unless $self->getClass();
    my $class = $self->getClass();

    print $fh "$depth</$class>\n";
}


=head2 B<export()>

Abstract class. Has to be implemented in sub-classes.

=cut

sub export{
    my $self = shift;
    $self->{'logger'}->LogError("Abstract method GePan::Exporter::XML::Writer->export() not implemented in sub-class.");
}

=head1 GETTER & SETTER METHODS

=head2 B<setFH(*FH)>

Sets open filehandle of output file.

=cut

sub setFH{
    my ($self,$fh) = @_;
    $self->{'fh'} = $fh;
}

=head2 B<getFH()>

Returns open filehandle of output file.

=cut

sub getFH{
    my $self = shift;
    return $self->{'fh'};
}

=head2 B<setDepth(int)>

Returns base depth of writer.

=cut


sub setDepth{
    my ($self,$depth) = @_;
    $self->{'depth'} = $depth;
}

=head2 B<getDepth()>

Returns base depth of writer.

=cut

sub getDepth{
    my $self = shift;
    return $self->{'depth'};
}


=head2 B<setClass(string)>

Sets object cass string of object to write.

=cut

sub setClass{
    my ($self,$class) = @_;
    $self->{'class'} = $class;
}

=head2 B<getClass()>

Returns object class string of object to write.

=cut

sub getClass{
    my $self = shift;
    return $self->{'class'};
}

=head2 <setParams(hash-ref)>

Sets $self->{key} to hash-ref->{key} for all keys of hash-ref.

=cut

sub setParams{
    my ($self,$h) = @_;
    foreach(%$h){
	$self->{$_} = $h->{$_};
    }
}


=head1 INTERNAL METHODS

=head2 B<_getTabs(int)>

Returns a string with int times '\t'.

=cut

sub _getTabs{
    my ($self,$int) = @_;

    my $string = "";

    for(my $i = 0;$i<=$int;$i++){
	$string.="\t";
    }
    return $string;
}

=head2 B<_writeLine($key,$value,int)>

Prints to file '<$key>$value</$key>' with int leading tabs.

=cut

sub _writeLine{
    my ($self,$key,$value,$depth) = @_;

    $self->{'logger'}->LogError("Exporter::XML::Writer::_writeLine() - No filehandle given for Writer::".$self->{'class'}) unless ref $self->{'fh'};
    my $fh = $self->{'fh'};
    for(my $i = 0;$i<=$depth;$i++){
        print $fh "\t";
    }
    print $fh "<$key>$value</$key>\n";
}


=head2 B<setLogger(GePan::Logger)>

Sets GePan::Logger object.

=cut

sub setLogger{
    my ($self,$l) = @_;
    $self->{'logger'} = $l;
}



1;

