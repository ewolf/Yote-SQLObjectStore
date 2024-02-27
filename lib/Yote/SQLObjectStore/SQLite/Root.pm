package Yote::SQLObjectStore::SQLite::Root;

use 5.16.0;

use warnings;

use base 'Yote::SQLObjectStore::SQLite::Obj';

sub _init {
    my $self = shift;
    $self->set_ref_hash( {} );
    $self->set_val_hash( {} );
}

# simply has a reference hash and a value hash
our %cols = (
    ref_hash => 'HASH_REF',
    val_hash => 'HASH_VALUE',
);

1;
