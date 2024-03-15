package Yote::SQLObjectStore::SQLite;

use 5.16.0;
use warnings;

use Yote::SQLObjectStore::SQLite::TableManagement;
use base 'Yote::SQLObjectStore::StoreBase';

use Carp qw(confess);
use DBI;

sub new {
    my ($pkg, %args ) = @_;
    $args{ROOT_PACKAGE} //= 'Yote::SQLObjectStore::SQLite::Root';
    return $pkg->SUPER::new( %args );
}

sub connect_sql {
    my ($pkg,%args) = @_;
    
    print  Data::Dumper->Dump([\%args]);
    my $file = "$args{BASE_DIRECTORY}/SQLITE.db";
    
    my $dbh = DBI->connect( "DBI:SQLite:dbname=$file", 
                            undef, 
                            undef, 
                            { PrintError => 0 } );
    die "$@ $!" unless $dbh;
    
    return $dbh;
    
}


sub make_all_tables {
    my $self = shift;
    my @sql = Yote::SQLObjectStore::SQLite::TableManagement->all_obj_tables_sql;
    $self->query_do( "BEGIN" );
    for my $s (@sql) { $self->query_do( $s ) }
    $self->query_do( "COMMIT" );
}


sub new_value_hash {
    my ($self, %vals) = @_;
    my $id = $self->next_id( 'HASH_VALUE' );
    return $self->tie_hash( {}, $id, 'VALUE', \%vals );
}

sub new_value_array {
    my ($self, @vals) = @_;
    my $id = $self->next_id( 'ARRAY_VALUE' );
    return $self->tie_array( [], $id, 'VALUE', \@vals );
}

sub tie_array {
    my ($self, $arry, $id, $valtype, $data) = @_;
    tie @$arry, 'Yote::SQLObjectStore::SQLite::Array', $id, $self, "ARRAY_$valtype", $valtype;
    $self->weak( $id, $arry );
    if ($data) {
        push @$arry, @$data;
    }
    return $arry;
}

sub tie_hash {
    my ($self, $hash, $id, $valtype, $data) = @_;

    tie %$hash, 'Yote::SQLObjectStore::SQLite::Hash', $id, $self, "HASH_$valtype", $valtype;
    $self->weak( $id, $hash );
    if ($data) {
        for my $key (keys %$data) {
            $hash->{$key} = $data->{$key};
        }
    }
    return $hash;
}

sub xform_out {
    my ($self, $value, $def) = @_;

    if( $def eq 'VALUE' || $value == 0 ) {
        return $value;
    } 

    # other option is a reference and the value is an id
    return $self->fetch( $value );
}

sub xform_in_full {
    my ($self, $value, $def, $field) = @_;
    
    my $ref = ref( $value );

    # field value is a string if VALUE, an id if a reference;
    my $field_value;
    if ($def =~ /^(HASH|ARRAY)_(VALUE|REF)$/) {
        my $data_type = $1;
        my $val_type = $2;

        my $table_name = $def;

        my $tied = $ref eq 'HASH' ? tied %$value : tied @$value;
        $field //= 
        die "$field only accepts $def" if $ref ne $1 or $tied->data_type ne $2;

        my $id = $tied->id;
        $field_value = $id;
    } elsif( $def eq 'VALUE' ) {
        die "accepts only values" if $ref;
        $field_value = $value;

    } elsif( $def eq 'REF' ) {
        if (defined $value) {
            confess "accepts only references" unless ref( $value );

            my $tied = $ref eq 'HASH' ? tied %$value : $ref eq 'ARRAY' ? tied @$value : $value;
            die "accepts only Yote::SQLObjectStore::BaseObj references" 
                unless $tied->isa( 'Yote::SQLObjectStore::BaseObj' )   || 
                $tied->isa( 'Yote::SQLObjectStore::BaseArray' ) ||
                $tied->isa( 'Yote::SQLObjectStore::BaseHash' );
            
            $field_value = $tied->id;
        } else {
            $field_value = 0;
        }
    } else {
        # this is the case where we have a specific class reference
        die "accepts only '$def' references" unless ref( $value ) && $value->isa( $def );
        $field_value = $value->id;
    }

    return $value, $field_value;
}

sub fetch_obj_from_sql {
    my ($self, $id) = @_;

    my ($table) = $self->query_line(
        "SELECT objtable FROM ObjectIndex WHERE id=?",
        $id );

    return undef unless $table;


    if ($table =~ /(ARRAY|HASH)_(REF|VALUE)/) {

        my $lookup = $2 eq 'REF' ? 'refid' : 'val';
        if ($1 eq 'ARRAY') {
            # create an empty tied array
            my $array = $self->tie_array( [], $id, $2 );

            my $array_data = (tied @$array)->data;

            # populate the tied array
            $self->apply_query_array
                ( "SELECT idx, $lookup FROM $table WHERE id=?",
                  [$id],
                  sub {
                      my ($idx, $v) = @_;
                      $array_data->[$idx] = $v;
                  }
                );
            return $array;
        } 

        # not an array, must be a hash

        # create an empty tied hash
        my $hash = $self->tie_hash( {}, $id, $2 );

        my $hash_data = (tied %$hash)->data;

        $self->apply_query_array
            ( "SELECT key, $lookup FROM $table WHERE id=?",
              [$id],
              sub {
                  my ($fld, $v) = @_;
                  $hash_data->{$fld} = $v;
              }
            );

        return $hash;
    }
    
    # otherwise is an object, so grab its data

    my $class = join "::", reverse split /_/, $table;
    my $cols = $class->cols;
    my @cols = keys %$cols;

    my $sql = "SELECT ".join(',', @cols )." FROM $table WHERE id=?";

    my (@ret) = $self->query_line( $sql, $id );

    my $obj = bless [
        $id,
        $table,
        { map { $cols[$_] => $ret[$_] } (0..$#cols) },
        $self,
        1, # HAS SAVE IN TABLE
        ], $class;

    $obj->_load;

    return $obj;
}

1;
