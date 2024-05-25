package Yote::SQLObjectStore::SQLite;

use 5.16.0;
use warnings;

use Yote::SQLObjectStore::SQLite::TableManager;
use base 'Yote::SQLObjectStore::StoreBase';

use Carp qw(confess);
use DBI;

use overload
    '""' => sub { my $self = shift; "$self->{OPTIONS}{BASE_DIRECTORY}/SQLITE.db" };

sub base_obj {
    'Yote::SQLObjectStore::SQLite::Obj';
}

sub insert_or_replace {
    'INSERT OR REPLACE ';
}

sub insert_or_ignore {
    'INSERT OR IGNORE ';
}


sub new {
    my ($pkg, %args ) = @_;
    $args{ROOT_PACKAGE} //= 'Yote::SQLObjectStore::SQLite::Root';

    return $pkg->SUPER::new( %args );
}

sub make_table_manager {
    my $self = shift;
    $self->{TABLE_MANAGER} = Yote::SQLObjectStore::SQLite::TableManager->new( $self );
}


sub connect_sql {
    my ($pkg,%args) = @_;
    
#    print  Data::Dumper->Dump([\%args, "CONNECT SQL"]);
    my $file = "$args{BASE_DIRECTORY}/SQLITE.db";
    
    my $dbh = DBI->connect( "DBI:SQLite:dbname=$file", 
                            undef, 
                            undef, 
                            { PrintError => 0 } );
    die "$@ $!" unless $dbh;
    
    return $dbh;
    
}

sub has_table {
    my ($self, $table_name) = @_;
    my ($has_table) = $self->query_line( "SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'TableVersions'" );
    return $has_table;
}

1;
