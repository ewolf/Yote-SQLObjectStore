package SQLite::Tool;

use base 'Yote::SQLObjectStore::SQLite::Obj';

our %cols = (
      name   => 'varchar(256)',
      url    => 'varchar(1026)',
      about  => 'text',
      average_user_rating => 'float',
);

1;
