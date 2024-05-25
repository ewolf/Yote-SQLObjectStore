package MariaDB::SomeThing;

use base 'Yote::SQLObjectStore::MariaDB::Obj';

our %cols = (
    name           => 'VARCHAR(100)',
    tagline        => 'VARCHAR(200)',
    brother        => '*',
    sister         => '*',
    sisters        => '*ARRAY_*SQLite::SomeThing',
    sisters_hash   => '*HASH<256>_*SQLite::SomeThing',
    lolov          => '*ARRAY_*ARRAY_VARCHAR(200)',
    something      => '*SQLite::SomeThing',
    some_ref_array => '*ARRAY_*',
    some_val_array => '*ARRAY_VARCHAR(100)',
    some_ref_hash  => '*HASH<256>_*',
    some_val_hash  => '*HASH<256>_VARCHAR(100)',
);

1;
