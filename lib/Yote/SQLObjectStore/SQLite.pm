package Yote::SQLObjectStore::SQLite;

use 5.16.0;
use warnings;

use Yote::SQLObjectStore::SQLite::TableManager;
use base 'Yote::SQLObjectStore::StoreBase';

use Carp qw(confess);
use DBI;

use overload
    '""' => sub { my $self = shift; "$self->{OPTIONS}{BASE_DIRECTORY}/SQLITE.db" };

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

# given a thing and its type definition
# return it and its internal value which will
# either be an object id or a string value
sub make_all_tables_sql {
    my $self = shift;
    my $manager = $self->get_table_manager;
    my @sql = $manager->generate_tables_sql( 'Yote::SQLObjectStore::SQLite::Obj' );
    return @sql;
}

sub check_type {
    my ($self, $value, $type_def) = @_;
    
    $value
        and
        $value->isa( 'Yote::SQLObjectStore::SQLite::Obj' ) ||
        $value->isa( 'Yote::SQLObjectStore::Array' ) ||
        $value->isa( 'Yote::SQLObjectStore::Hash' ) 
        and
        $value->is_type( $type_def );
}

sub has_table {
    my ($self, $table_name) = @_;
    my ($has_table) = $self->query_line( "SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'TableVersions'" );
    return $has_table;
}

1;
