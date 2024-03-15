package Yote::SQLObjectStore::MariaDB::TableManager;

use 5.16.0;
use warnings;

use File::Grep qw(fgrep fmap fdo);
use Module::Load::Conditional qw(requires can_load);
use Yote::SQLObjectStore::MariaDB::TableManager;

# generate sql to make tables
# takes a list of subclasses of Yote::SQLObjectStore::Obj 
# to make tables for. also makes tables for Hash* and Array*
sub generate_base_sql {
    my ($pkg) = @_;
    
    my @tables;

    # create object index and root
    push @tables, <<"END";
CREATE TABLE IF NOT EXISTS ObjectIndex ( 
    id       INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    live     TINYINT(1) DEFAULT 0,
    objtable VARCHAR(512)
);
END

    # make different array and hash tables
    push @tables, "CREATE TABLE IF NOT EXISTS HASH_REF (id INT UNSIGNED, key VARCHAR(256), refid INT UNSIGNED, UNIQUE (id,key))";

    push @tables, "CREATE TABLE IF NOT EXISTS HASH_VALUE (id INT UNSIGNED, key VARCHAR(256), val TEXT, UNIQUE (id,key))";

    push @tables, "CREATE TABLE IF NOT EXISTS ARRAY_REF (id INT UNSIGNED, idx INT UNSIGNED, refid INT UNSIGNED, UNIQUE (id,idx))";

    push @tables, "CREATE TABLE IF NOT EXISTS ARRAY_VALUE (id INT UNSIGNED, idx INT UNSIGNED, val, UNIQUE (id,idx))";


    return @tables;
}


1;
