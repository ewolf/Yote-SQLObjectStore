package Yote::SQLObjectStore::MariaDB;

use 5.16.0;
use warnings;

use Yote::SQLObjectStore::MariaDB::TableManager;
use base 'Yote::SQLObjectStore::StoreBase';

use Carp qw(confess);
use DBI;
use DBD::MariaDB;

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

sub store_obj_data_to_sql {
    my ($self, $obj ) = @_;

    my $ref = $self->tied_item($obj);

    my ($id, $table, @queries) = $ref->save_sql;

    for my $q (@queries) {
        my ($update_obj_table_sql, @qparams) = @$q;
        $self->_query_do( $update_obj_table_sql, @qparams );
    }
}

sub record_count {
    my $self = shift;

    my ($count) = $self->_query_line( "SELECT count(*) FROM ObjectIndex WHERE live=1" );
    
    return $count;
}


sub make_table_manager {
    my $self = shift;
    $self->{TABLE_MANAGER} = Yote::SQLObjectStore::MariaDB::TableManager->new( $self );
}


sub make_all_tables_sql {
    my $self = shift;
    my $manager = $self->get_table_manager;
    my @sql = $manager->generate_tables_sql( 'Yote::SQLObjectStore::MariaDB::Obj' );
    return @sql;
}

sub has_table {
    my ($self, $table_name) = @_;
    my ($has_table) = $self->query_line( "SHOW TABLES LIKE ?", $table_name );
    return $has_table;
}


sub new_obj($*@) {
    my ($self, $pkg, %args) = @_;
    my $package_file = $pkg;
    $package_file =~ s/::/\//g;
    require "$package_file.pm";

    my $table = join '_', reverse split /::/, $pkg;

    my $id = $self->next_id( $table );

    my $obj_data = {};
    my $obj = bless {
        ID => $id,

        data => $obj_data,
        has_first_save => 0,
        store => $self,
        table => $table,
    }, $pkg;

    $obj->_init;

    $self->weak( $id, $obj );
    $self->dirty( $id, $obj );

    if (%args) {
        my $cols = $pkg->cols;

        for my $input_field (keys %args) {
            if ( my $coldef = $cols->{$input_field} ) {
                $obj_data->{$input_field} = $self->xform_in( $args{$input_field}, $coldef );
            } else {
                warn "'$input_field' does not exist for object with package $pkg";
            }
        }
    }

    return $obj;
}

sub new_ref_hash {
    my ($self, %refs) = @_;
    my $id = $self->next_id( 'HASH_REF' );
    return $self->tie_hash( {}, $id, 'REF', \%refs );
}

sub new_value_hash {
    my ($self, %vals) = @_;
    my $id = $self->next_id( 'HASH_VALUE' );
    return $self->tie_hash( {}, $id, 'VALUE', \%vals );
}

sub new_ref_array {
    my ($self, @refs) = @_;
    my $id = $self->next_id( 'ARRAY_REF' );
    return $self->tie_array( [], $id, 'REF', \@refs );
}

sub new_value_array {
    my ($self, @vals) = @_;
    my $id = $self->next_id( 'ARRAY_VALUE' );
    return $self->tie_array( [], $id, 'VALUE', \@vals );
}

sub tie_array {
    my ($self, $arry, $id, $valtype, $data) = @_;
    tie @$arry, 'Yote::SQLObjectStore::Array', $id, $self, "ARRAY_$valtype", $valtype;
    $self->weak( $id, $arry );
    if ($data) {
        push @$arry, @$data;
    }
    return $arry;
}

sub tie_hash {
    my ($self, $hash, $id, $valtype, $data) = @_;

    tie %$hash, 'Yote::SQLObjectStore::Hash', $id, $self, "HASH_$valtype", $valtype;
    $self->weak( $id, $hash );
    if ($data) {
        for my $key (keys %$data) {
            $hash->{$key} = $data->{$key};
        }
    }
    return $hash;
}

sub check_type {
    my ($self, $value, $type_def) = @_;
    
    $value
        and
        $value->isa( 'Yote::SQLObjectStore::MariaDB::Obj' ) ||
        $value->isa( 'Yote::SQLObjectStore::Array' ) ||
        $value->isa( 'Yote::SQLObjectStore::Hash' ) 
        and
        $value->is_type( $type_def );
}


1;
