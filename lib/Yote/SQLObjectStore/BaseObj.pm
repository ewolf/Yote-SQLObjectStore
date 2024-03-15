package Yote::SQLObjectStore::BaseObj;

use 5.16.0;

no warnings 'uninitialized';

use MIME::Base64;

use constant {
    ID             => 0,
    DATA_TYPE      => 1,
    DATA           => 2,
    STORE          => 3,
    HAS_FIRST_SAVE => 4,
};

#
# The string version of the objectstore object is simply its id. 
# This allows object ids to easily be stored as hash keys.
#
use overload
    '""' => sub { my $self = shift; $self->[ID] },
    eq   => sub { ref($_[1]) && $_[1]->[ID] == $_[0]->[ID] },
    ne   => sub { ! ref($_[1]) || $_[1]->[ID] != $_[0]->[ID] },
    '=='   => sub { ref($_[1]) && $_[1]->[ID] == $_[0]->[ID] },
    '!='   => sub { ! ref($_[1]) || $_[1]->[ID] != $_[0]->[ID] },
    fallback => 1;

sub new {
    my ($pkg, $id, $table, $data, $store, $has_first_save) = @_;
    my $obj = bless [
        $id, $table, $data, $store, $has_first_save || 0
    ], $pkg;
    if ($has_first_save) {
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
    my ($me, @suffix) = @_;
    my $pkg = ref($me) ? ref($me) : $me;
    my (@parts) = reverse split /::/, $pkg;
    return join( "_", @parts, @suffix );
}

#
# Stub methods to override
#
sub _init {}
sub _load {}

#
# Instance methods
#
sub id {
    return shift->[ID];
}

sub _has_first_save {
    return shift->[HAS_FIRST_SAVE];
}

sub data_type {
    return shift->[DATA_TYPE];
}

sub data {
    return shift->[DATA];
}

sub store {
    return shift->[STORE];
}

sub fields {
    return [sort keys %{shift->cols}];
}

sub dirty {
    my $self = shift;
    $self->[STORE]->dirty( $self->[ID], $self );
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
