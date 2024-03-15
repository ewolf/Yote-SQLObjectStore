package Yote::SQLObjectStore::SQLite::Root;

use 5.16.0;

use warnings;

use base 'Yote::SQLObjectStore::SQLite::Obj';

sub _init {
    my $self = shift;
print STDERR Data::Dumper->Dump([$self,"MEMEMEM"]);
    my $store = $self->store;
    $self->set_ref_hash( $store->new_hash('*HASH<256>_*') );
    $self->set_val_hash( $store->new_hash('*HASH<256>_VALUE') );
}

# simply has a reference hash and a value hash
our %cols = (
    ref_hash => '*HASH<256>_*',
    val_hash => '*HASH<256>_VALUE',
);

1;
