package SomeThing;

use base 'Yote::SQLObjectStore::SQLite::Obj';

# simply has a reference hash and a value hash
our %cols = (
    name => 'VALUE',
    brother => 'REF',
    sister => 'REF',
);

1;
