package Yote::ObjectStore::Caching;

use 5.14.0;

use base 'Data::ObjectStore';

use constant {
    RECORD_STORE => 0,
    DIRTY	 => 1,
    WEAK	 => 2,
    CACHE	 => 3,
    CACHE_SIZE	 => 4,

    DEFAULT_CACHE_SIZE => 100_000,
};


sub open_store {
    my ($pkg, $record_store, $cache_size) = @_;
    unless (ref $record_store) {
	$record_store = Data::RecordStore->open_store( $record_store );
    }
    $cache_size //= DEFAULT_CACHE_SIZE;
    return bless [
	$record_store,
	{},
	{},
	{},
	$cache_size,
	], $pkg;
} #open_store

sub uncache {
    my ($self, $obj) = @_;
    
    return delete $self->[CACHE]->{$self->id($obj)};
}

sub fetch {
    my ($self, $id) = @_;
    my $item = $self->[CACHE]{$id};
    unless ($item) {
	$item = $self->SUPER::fetch($id);
	$self->add_to_cache( $id, $item );
    }
    return $item;
} #fetch

sub weak {
    my ($self,$id,$ref) = @_;
    $self->SUPER::weak( $id, $ref );
    $self->add_to_cache( $id, $ref );
}

sub add_to_cache {
    my ($self,$id,$ref) = @_;
    my $cache = $self->[CACHE];
    if (scalar(keys %$cache) > $self->[CACHE_SIZE]) {
	# delete something at random
	my ($k,$v) = each %$cache;
	delete $cache->{$k};
    }
    $cache->{$id} = $ref;
} #add_to_cache

"PACMAN";
