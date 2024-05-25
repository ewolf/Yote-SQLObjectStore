package Yote::SQLObjectStore::MariaDB::TableManager;

use 5.16.0;
use warnings;

use File::Grep qw(fgrep fmap fdo);
use Module::Load::Conditional qw(requires can_load);
use base 'Yote::SQLObjectStore::TableManager';

sub new_column {
    my ($self, $table_name, $column_name, $column_def) = @_;
    "ALTER TABLE $table_name ADD COLUMN $column_name $column_def";
}

sub change_column {
    my ($self, $table_name, $column_name, $column_def) = @_;
    return 
        "ALTER TABLE $table_name CHANGE COLUMN $column_name $column_name $column_def";
}

sub archive_column {
    my ($self, $table_name, $column_name) = @_;
    "ALTER TABLE $table_name RENAME COLUMN $column_name TO ${column_name}_DELETED",
}

sub create_object_index_sql {
    return <<"END";
CREATE TABLE IF NOT EXISTS ObjectIndex ( 
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tablehandle VARCHAR(256),
    live        TINYINT
);
END
}

sub create_table_defs_sql {
    return <<"END";
CREATE TABLE IF NOT EXISTS TableDefs ( 
    id      INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name    VARCHAR(256),
    version FLOAT,
    UNIQUE (name)
);
END
}

sub create_table_versions_sql {
    return <<"END";
CREATE TABLE IF NOT EXISTS TableVersions (
    name         VARCHAR(256),
    version      INT,
    create_table TEXT,
    UNIQUE (name, version)
);
END
}

1;
