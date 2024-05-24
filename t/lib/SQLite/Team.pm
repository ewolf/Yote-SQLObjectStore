package SQLite::Team;

use base 'Yote::SQLObjectStore::SQLite::Obj';

our %cols = (
       name  => 'varchar(256)',
       quote => 'varchar(1026)',
       favorite_tools => '*ARRAY_*cl::tree:tool',
       preferences => '*HASH<256>_*',
);

1;
