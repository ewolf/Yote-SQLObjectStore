package Yote::SQLObjectStore::SQLite::TableManagement;

use 5.16.0;
use warnings;

# generate sql to make tables
# takes a list of subclasses of Yote::SQLObjectStore::Obj 
# to make tables for. also makes tables for Hash* and Array*
sub generate_sql {
    my ($pkg, @packages) = @_;
    
    my @tables;

    # create object index and root
    push @tables, <<"END";
CREATE TABLE IF NOT EXISTS ObjectIndex ( 
    id INT PRIMARY KEY AUTOINCREMENT,
    table TEXT
);
END

    # make different array and hash tables
    push @tables, "CREATE TABLE IF NOT EXISTS HASH_REF (id INT PRIMARY KEY, key TEXT, refid INT, UNIQUE (id,key))";
    push @tables, "CREATE TABLE IF NOT EXISTS HASH_VALUE (id INT PRIMARY KEY, key TEXT, val, UNIQUE (id,key))";

    push @tables, "CREATE TABLE IF NOT EXISTS ARRAY_REF (id INT PRIMARY KEY, idx INT, refid INT, UNIQUE (id,idx))";
    push @tables, "CREATE TABLE IF NOT EXISTS ARRAY_VALUE (id INT PRIMARY KEY, idx INT, val, UNIQUE (id,idx))";

    for my $obj_pkg (@packages) {
        
    }

    return @tables;
}

1;
