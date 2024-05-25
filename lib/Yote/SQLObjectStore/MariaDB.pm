package Yote::SQLObjectStore::MariaDB;

use 5.16.0;
use warnings;

use Yote::SQLObjectStore::MariaDB::TableManager;
use base 'Yote::SQLObjectStore::StoreBase';

use Carp qw(confess);
use DBI;
use DBD::MariaDB;

sub base_obj {
    'Yote::SQLObjectStore::MariaDB::Obj';
}

sub new {
    my ($pkg, %args ) = @_;
    $args{ROOT_PACKAGE} //= 'Yote::SQLObjectStore::MariaDB::Root';
    return $pkg->SUPER::new( %args );
}

sub connect_sql {
    my ($pkg,%args) = @_;
    
    my $dbh = DBI->connect( "DBI:MariaDB".($args{dbname} ? ":dbname=$args{dbname}" : ""),
                            $args{username} || 'wolf', 
                            $args{password} || 'boogers', 
                            { PrintError => 0 } );
    die "$@ $!" unless $dbh;
    
    return $dbh;
    
}

sub make_table_manager {
    my $self = shift;
    $self->{TABLE_MANAGER} = Yote::SQLObjectStore::MariaDB::TableManager->new( $self );
}


sub has_table {
    my ($self, $table_name) = @_;
    my ($has_table) = $self->query_line( "SHOW TABLES LIKE ?", $table_name );
    return $has_table;
}



1;
