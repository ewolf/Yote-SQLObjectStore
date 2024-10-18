package Yote::SQLObjectStore::SQLite::Root;

use 5.16.0;

use warnings;

use base 'Yote::SQLObjectStore::SQLite::Obj';

# simply has a reference hash and a value hash
our %cols = (
    ref_hash => '*HASH<256>_*',
    val_hash => '*HASH<256>_VALUE',
);

1;
