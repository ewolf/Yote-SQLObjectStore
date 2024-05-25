package SQLite::SomeThing;

use base 'Yote::SQLObjectStore::SQLite::Obj';

sub _init {
    my $self = shift;
    $self->set_some_ref_tied_hash( $store->new_tied_hash('*HASH<256>_*') );
    $self->set_some_ref_tied_array( $store->new_tied_hash('*ARRAY_*') );
    $self->set_some_val_tied_hash( $store->new_tied_hash('*HASH<256>_VALUE') );
    $self->set_some_val_tied_array( $store->new_tied_hash('*ARRAY_VALUE') );
}

# simply has a reference hash and a value hash
our %cols = (
    name           => 'VALUE',
    tagline        => 'VALUE',
    brother        => '*',
    sister         => '*',
    sisters        => '*ARRAY_*SQLite::SomeThing',
    sisters_hash   => '*HASH<256>_*SQLite::SomeThing',
    lolov          => '*ARRAY_*ARRAY_VALUE',
    something      => '*SQLite::SomeThing',
    some_ref_tied_array => '*ARRAY_*',
    some_val_tied_array => '*ARRAY_VALUE',
    some_ref_tied_hash  => '*HASH<256>_*',
    some_val_tied_hash  => '*HASH<256>_VALUE',

    some_ref_lookup_array => '^ARRAY_*',
    some_val_lookup_array => '^ARRAY_VALUE',
    some_ref_lookup_hash  => '^HASH<256>_*',
    some_val_lookup_hash  => '^HASH<256>_VALUE',
);

1;
