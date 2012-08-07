package GePan::Collection::Sequence;
use base qw(GePan::Collection);

=head1 NAME

GePan::Collection::Sequence

=head1 DESCRIPTION

Class class of GePan::Sequence objects


=head1 METHODS

=head2 B<sortByStart()>

Sorts all sequences in collection by ascending start. Sequences can then be retrieved using methods getList() or getNextElement().

=cut

sub sortByStart{
    my $self = shift;
    my $list = $self->{'list'};
    my $sorted = [];
    foreach my $seq (sort{$a->{'start'}<=>$b->{'start'}}(@$list)){
	push @$sorted,$seq;
    }
    $self->{'list'} = $sorted;
}


=head1 GETTER & SETTER METHODS

=head2 B<getElementsByAttributeHash(ref)>

Returns GePan::Collection::Sequence object with all sequences that match given attribute keys and values.

=cut

sub getElementsByAttributeHash{
    my ($self,$h) = @_;

    my $collection = GePan::Collection::Sequence->new();

    foreach my $sequence (@{$self->{'list'}}){
	my $true = 1;
	foreach(keys(%$h)){
	    if(!($sequence->{$_})||($sequence->{$_} ne $h->{$_})){
		$true = 0;
	    }
	}
	$collection->addElement($sequence) unless !($true);
    }

    return $collection unless !(scalar(@{$collection->getList()}));
    return 0;
}

1;
