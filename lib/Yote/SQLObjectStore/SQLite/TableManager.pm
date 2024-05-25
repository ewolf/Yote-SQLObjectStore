package Yote::SQLObjectStore::SQLite::TableManager;

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
    # not really needed in sqlite, since there is only one data
    # type, but here we go
    return 
        "ALTER TABLE $table_name RENAME COLUMN $column_name ${column_name}_mv",
        "ALTER TABLE $table_name ADD COLUMN $column_name $column_def",
        "UPDATE $table_name SET $column_name = ${column_name}_mv",
        "ALTER TABLE $table_name DROP COLUMN ${column_name}_mv";
}

sub generate_tables_sql {
    my ($self, $base_obj_package) = @_;
    my @sql = $self->SUPER::generate_tables_sql( $base_obj_package );
    for my $sqlist (@sql) {
        for my $sql (@$sqlist) {
            $sql =~ s/^INSERT IGNORE/INSERT OR IGNORE/;
        }
    }
    return @sql;
}

sub archive_column {
    my ($self, $table_name, $column_name) = @_;
    "ALTER TABLE $table_name RENAME COLUMN $column_name ${column_name}_DELETED",
}

sub create_object_index_sql {
    return <<"END";
CREATE TABLE IF NOT EXISTS ObjectIndex ( 
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    tablehandle TEXT,
    live        BOOLEAN
);
END
}

sub create_table_defs_sql {
    return <<"END";
CREATE TABLE IF NOT EXISTS TableDefs ( 
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name    TEXT,
    version FLOAT,
    UNIQUE (name)
);
END
}

sub create_table_versions_sql {
    return <<"END";
CREATE TABLE IF NOT EXISTS TableVersions (
    name         TEXT,
    version      INT,
    create_table TEXT,
    UNIQUE (name, version)
);
END
}

1;
