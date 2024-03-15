package Yote::SQLObjectStore::MariaDB::Root;

use 5.16.0;

use warnings;

use base 'Yote::SQLObjectStore::MariaDB::Obj';

sub _init {
    my $self = shift;
    my $store = $self->store;
    $self->set_ref_hash( $store->new_ref_hash );
    $self->set_val_hash( $store->new_value_hash );
}

# simply has a reference hash and a value hash
our %cols = (
    ref_hash => 'HASH_REF',
    val_hash => 'HASH_VALUE',
);

1;
