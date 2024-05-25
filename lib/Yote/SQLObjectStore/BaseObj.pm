package Yote::SQLObjectStore::BaseObj;

use 5.16.0;
use warnings;

no warnings 'uninitialized';

use base 'Yote::SQLObjectStore::BaseStorable';

sub new {
    my $pkg = shift;
    # in this case datatype is table
    my $obj = $pkg->SUPER::new( @_ );
    if ($obj->has_first_save) {
        $obj->_load;
    } else {
        $obj->_init;
    }
    return $obj;
}


#
# columns as a hash of column name -> "SQL type" ||  { type => "SQL type", ... extra indexing stuff i guess? }
#
sub cols {
    my $me = shift;
    my $pkg = ref($me) ? ref($me) : $me;
    no strict 'refs';
    return {%{"${pkg}::cols"}};
}

# returns column names, alphabetically sorted
sub col_names {
    my $cols = shift->cols;
    return sort keys %$cols;
}

# return table name, which is the reverse of 
# the package path (most specific first)
# plus any suffix parts
sub table_name {
    my ($self, @suffix) = @_;
    my $pkg = ref($self);
    my (@parts) = reverse split /::/, $pkg;
    return join( "_", @parts, @suffix );
}

#
# Stub methods to override
#
sub _init {}
sub _load {}

sub fields {
    return [sort keys %{shift->cols}];
}

sub save_sql {
    my ($self) = @_;
    
    my $id = $self->id;
    my $data = $self->data;
    my $table = $self->table_name;
    my @col_names = $self->col_names;

    my ($sql);

    my @qparams = map { $data->{$_} } @col_names;
    if( $self->has_first_save ) {
        $sql = "UPDATE $table SET ".
            join(',',  map { "$_=?" } @col_names ).
            " WHERE id=?";
        push @qparams, $id;
    } 
    else {
        $sql = "INSERT INTO $table (".
            join(',', 'id', @col_names).") VALUES (".
            join(',', ('?') x (1+@col_names) ).
           ")";
        unshift @qparams, $id;
    }
    return [$sql, @qparams];
}

sub set {
    my ($self,$field,$value) = @_;

    # check the col
    my $cols = $self->cols;
    my $def = $cols->{$field};

    die "No field '$field' in ".ref($self) unless $def;

    my $store = $self->store;

    my ($item, $field_value) = $store->xform_in_full( $value, $def );

    my $data = $self->data;

    my $dirty = $data->{$field} ne $field_value;

    $data->{$field} = $field_value;

    if ($dirty) {
        $self->dirty;
    }

    return $item;    
}

sub get {
    my ($self,$field,$default) = @_;

    my $cols = $self->cols;
    my $def = $cols->{$field};

    die "No field '$field' in ".ref($self) unless $def;

    my $data = $self->data;

print STDERR Data::Dumper->Dump([$field,$default,$data,"GETMEA"]);

    if ((! exists $data->{$field}) and defined($default)) {
	return $self->set($field,$default);
    }

    return $self->store->xform_out( $data->{$field}, $def );
} #get


sub AUTOLOAD {
    my( $s, $arg ) = @_;
    my $func = our $AUTOLOAD;
    if ( $func =~ /:set_(.*)/ ) {
        my $fld = $1;
        no strict 'refs';
        *$AUTOLOAD = sub {
            my( $self, $val ) = @_;
            $self->set( $fld, $val );
        };
        use strict 'refs';
        goto &$AUTOLOAD;
    }
    elsif( $func =~ /:get_(.*)/ ) {
        my $fld = $1;
        no strict 'refs';
        *$AUTOLOAD = sub {
            my( $self, $init_val ) = @_;
            $self->get( $fld, $init_val );
        };
        use strict 'refs';
        goto &$AUTOLOAD;
    }
    elsif( $func !~ /:DESTROY$/ ) {
        die "Yote::ObjectStore::Obj::$func : unknown function '$func'.";
    }

} #AUTOLOAD

1;
