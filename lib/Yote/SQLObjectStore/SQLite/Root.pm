package Yote::SQLObjectStore::SQLite::Root;

use 5.16.0;

use warnings;

use base 'Yote::SQLObjectStore::SQLite::Obj';

sub _init {
    my $self = shift;
    my $store = $self->store;
    $self->set_ref_hash( $store->make_ref_hash );
    $self->set_val_hash( $store->make_value_hash );
    print STDERR Data::Dumper->Dump([$self,"INIT ($self)"]);
}

# simply has a reference hash and a value hash
our %cols = (
    ref_hash => 'HASH_REF',
    val_hash => 'HASH_VALUE',
);

1;
