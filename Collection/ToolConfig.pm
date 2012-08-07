package GePan::Collection::ToolConfig;
use base qw(GePan::Collection);

use strict;
use Data::Dumper;
use GePan::ToolConfig;

=head1 NAME

GePan::Collection::ToolConfig

=head1 DESCRIPTION

Class for storing GePan::ToolConfig objects

=head1 METHODS

=head2 B<is_registered(ID)>

Returns 1 if a tool of id ID is registered 0 otherwise;

=cut

sub is_registered{
    my ($self,$id) = @_;
    if(ref((grep{$_ eq $id}@{$self->{'list'}})[0])){
	return 1;
    }
    else{
	return 0;
    }
}




=head1 GETTER & SETTER METHODS

=head2 <getElementsByAttributeHash(ref)>

Returns GePan::Collection::ToolConfig containing all elements that match the given attributes.

=cut

sub getElementsByAttributeHash{
    my ($self,$params) = @_;
    my $collection = GePan::Collection::ToolConfig->new();
    $collection->setLogger($self->{'logger'});
    foreach my $element (@{$self->{'list'}}){
	my $true = 1;
	foreach my $att (keys(%$params)){
	    if($element->{$att} ne $params->{$att}){
		$true = 0;
		last;
	    }
	}
	$collection->addElement($element) if $true;
    }
    return $collection;
}

1;
