package Yote::SQLObjectStore::SQLite;

use 5.16.0;
use warnings;

use Yote::SQLObjectStore::SQLite::TableManagement;
use base 'Yote::SQLObjectStore::StoreBase';

use Carp qw(confess);
use DBI;

use constant {
    INSERT_QUERY => "INSERT INTO ObjectIndex (objtable,live) VALUES(?,1)",
};

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

sub make_all_tables {
    my $self = shift;
    my @sql = Yote::SQLObjectStore::SQLite::TableManagement->all_obj_tables_sql;
    $self->_query_do( "BEGIN" );
    for my $s (@sql) { $self->_query_do( $s ) }
    $self->_query_do( "COMMIT" );
}

sub new_obj($*@) {
    my ($self, $pkg, %args) = @_;
    my $package_file = $pkg;
    $package_file =~ s/::/\//g;
    require "$package_file.pm";

    my $table = join '_', reverse split /::/, $pkg;

    my $id = $self->_insert_get_id( INSERT_QUERY, $table );

    my $obj_data = {};
    my $obj = bless [
        $id,
        $obj_data,
        $self,
        {}, #volatile
        0, # NO SAVE IN OBJ TABLE YET 
        ], $pkg;

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
    my $id = $self->_insert_get_id( INSERT_QUERY, 'HASH_REF' );
    return $self->tie_hash( {}, $id, 'REF', \%refs );
}

sub new_value_hash {
    my ($self, %vals) = @_;
    my $id = $self->_insert_get_id( INSERT_QUERY, 'HASH_VALUE' );
    return $self->tie_hash( {}, $id, 'VALUE', \%vals );
}

sub new_ref_array {
    my ($self, @refs) = @_;
    my $id = $self->_insert_get_id( INSERT_QUERY, 'ARRAY_REF' );
    return $self->tie_array( [], $id, 'REF', \@refs );
}

sub new_value_array {
    my ($self, @vals) = @_;
    my $id = $self->_insert_get_id( INSERT_QUERY, 'ARRAY_VALUE' );
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

sub xform_in {
    my $self = shift;
    my $encoded = $self->xform_in_full(@_);
    return $encoded;
}

sub xform_in_full {
    my ($self, $value, $def) = @_;
    
    my $ref = ref( $value );

    # field value is a string if VALUE, an id if a reference;
    my $field_value;
    if ($def =~ /^(HASH||ARRAY)_(VALUE|REF)$/) {
        my $data_type = $1;
        my $val_type = $2;
        die "accepts only hashes" if $value && $ref ne $1;

        my $table_name = $def;

        my $tied = $ref eq 'HASH' ? tied %$value : tied @$value;
        my $id;
        if ($tied) {
            $id = $tied->id;
        }
        else {
            $id = $self->id_for_item( $def );

            my $data_structure = $data_type eq 'HASH' ?
                $self->tie_hash( {}, $id, $val_type, $value ) :
                $self->tie_array( [], $id, $val_type, $value );

            $self->dirty( $id );
            $value = $data_structure;
        }
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
            warn "this could be a big decision. allow undefined references?";
            $field_value = 0;
        }
    } else {
        die "accepts only '$def' references" unless ref( $value ) && $value->isa( $def );
        $field_value = $value->id;
    }
    return $value, $field_value;
}

sub fetch_obj_from_sql {
    my ($self, $id) = @_;

    my ($table) = $self->_query_line(
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
            $self->_apply_query_array
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

        $self->_apply_query_array
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

    my (@ret) = $self->_query_line( $sql, $id );

    my $obj = bless [
        $id,
        { map { $cols[$_] => $ret[$_] } (0..$#cols) },
        $self,
	{},
        1, # HAS SAVE IN TABLE
        ], $class;

    return $obj;
}

sub next_id {
    my ($self, $table) = @_;
    
    return $self->_insert_get_id( INSERT_QUERY, $table );
}

# --------- DB FUNS -------

sub _sth {
    my ($self, $query ) = @_;

    my $stats = $self->statements;
    my $dbh   = $self->dbh;
    my $sth   = ($stats->{$query} //= $dbh->prepare( $query ));
    $sth or die $dbh->errstr;

    return $sth;
}

sub _insert_get_id {
    my ($self, $query, @qparams ) = @_;
    my $dbh = $self->dbh;
    my $sth = $self->_sth( $query );
    my $res = $sth->execute( @qparams );
    if (!defined $res) {
        die $sth->errstr;
    }
    my $id = $dbh->last_insert_id;
    return $id;    
}


sub _query_all {
    my ($self, $query, @qparams ) = @_;
    print STDERR Data::Dumper->Dump([$query,\@qparams,"query_all"]);
    my $dbh = $self->dbh;
    my $sth = $dbh->prepare( $query );
    if (!defined $sth) {
        die $dbh->errstr;
    }
    my $res = $sth->execute( @qparams );
    if (!defined $res) {
        die $sth->errstr;
    }
    return $sth->fetchall_hashref('id');
}


sub _query_do {
    my ($self, $query, @qparams ) = @_;
    print STDERR Data::Dumper->Dump([$query,\@qparams,"query do"]);
    my $dbh = $self->dbh;
    my $sth = $dbh->prepare( $query );
    if (!defined $sth) {
        die $dbh->errstr;
    }
    my $res = $sth->execute( @qparams );
    if (!defined $res) {
use Carp 'longmess'; print STDERR Data::Dumper->Dump([longmess]);
        die $sth->errstr;
    }
    my $id = $dbh->last_insert_id;
    return $id;
}

sub _query_line {
    my ($self, $query, @qparams ) = @_;
    print STDERR Data::Dumper->Dump([$query,\@qparams,"query line"]);    
    my $sth = $self->_sth( $query );

    my $res = $sth->execute( @qparams );
    if (!defined $res) {
        die $sth->errstr;
    }
    my @ret = $sth->fetchrow_array;
    return @ret;
}

sub _apply_query_array {
    my ($self, $query, $qparams, $eachrow_fun ) = @_;
    my $sth = $self->_sth( $query );
    my $res = $sth->execute( @$qparams );
    if (!defined $res) {
        die $sth->errstr;
    }
    while ( my @arry = $sth->fetchrow_array ) {
        $eachrow_fun->(@arry);
    }
}

1;
