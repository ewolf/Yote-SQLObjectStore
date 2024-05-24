package SQLite::SomeThing;

use base 'Yote::SQLObjectStore::SQLite::Obj';

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
    some_ref_array => '*ARRAY_*',
    some_val_array => '*ARRAY_VALUE',
    some_ref_hash  => '*HASH<256>_*',
    some_val_hash  => '*HASH<256>_VALUE',
);

1;
