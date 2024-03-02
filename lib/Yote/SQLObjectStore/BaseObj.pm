package Yote::SQLObjectStore::BaseObj;

use 5.16.0;

no warnings 'uninitialized';

use MIME::Base64;

use constant {
    ID	     => 0,
    DATA     => 1,
    STORE    => 2,
    VOL      => 3,
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

#
# columns as a hash of column name -> "SQL type" ||  { type => "SQL type", ... extra indexing stuff i guess? }
#
sub cols {
    my $me = shift;
    my $pkg = ref($me) || $me;
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
    my $pkg = ref($me) || $me;
    my (@parts) = reverse split /::/, $pkg;
    return join( "_", @parts, @suffix );
}

sub make_table_sql {
    die "override me";
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

sub data {
    return shift->[DATA];
}

sub store {
    return shift->[STORE];
}

sub fields {
    return [keys %{shift->[DATA]}];
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

    $dirty && $self->dirty;

    return $item;    
}

sub get {
    my ($self,$field,$default) = @_;

    my $data = $self->data;
    if ((! exists $data->{$field}) and defined($default)) {
	return $self->set($field,$default);
    }
    my $cols = $self->cols;
    my $def = $cols->{$field};

    return $self->store->xform_out( $data->{$field}, $def );
} #get


sub AUTOLOAD {
    my( $s, $arg ) = @_;
    my $func = our $AUTOLOAD;
    if( $func =~/:add_to_(.*)/ ) {
        my( $fld ) = $1;
        no strict 'refs';
        *$AUTOLOAD = sub {
            my( $self, @vals ) = @_;
            my $get = "get_$fld";
            my $arry = $self->$get([]); # init array if need be
            push( @$arry, @vals );
	    return scalar(@$arry);
        };
        use strict 'refs';
        goto &$AUTOLOAD;
    } #add_to
    elsif( $func =~/:add_once_to_(.*)/ ) {
        my( $fld ) = $1;
        no strict 'refs';
        *$AUTOLOAD = sub {
            my( $self, @vals ) = @_;
            my $get = "get_$fld";
            my $arry = $self->$get([]); # init array if need be
            for my $val ( @vals ) {
                unless( grep { $val eq $_ } @$arry ) {
                    push @$arry, $val;
                }
            }
	    return scalar(@$arry);
        };
        use strict 'refs';
        goto &$AUTOLOAD;
    } #add_once_to
    elsif( $func =~ /:remove_from_(.*)/ ) { #removes the first instance of the target thing from the list
        my $fld = $1;
        no strict 'refs';
        *$AUTOLOAD = sub {
            my( $self, @vals ) = @_;
            my $get = "get_$fld";
            my $arry = $self->$get([]); # init array if need be
            my( @ret );
          V:
            for my $val (@vals ) {
                for my $i (0..$#$arry) {
                    if( $arry->[$i] eq $val ) {
                        push @ret, splice @$arry, $i, 1;
                        next V;
                    }
                }
            }
            return @ret;
        };
        use strict 'refs';
        goto &$AUTOLOAD;
    }
    elsif( $func =~ /:remove_all_from_(.*)/ ) { #removes the first instance of the target thing from the list
        my $fld = $1;
        no strict 'refs';
        *$AUTOLOAD = sub {
            my( $self, @vals ) = @_;
            my $get = "get_$fld";
            my $arry = $self->$get([]); # init array if need be
            my @ret;
            for my $val (@vals) {
                for( my $i=0; $i<=@$arry; $i++ ) {
                    if( $arry->[$i] eq $val ) {
                        push @ret, splice @$arry, $i, 1;
                        $i--;
                    }
                }
            }
            return @ret;
        };
        use strict 'refs';
        goto &$AUTOLOAD;
    }
    elsif ( $func =~ /:set_(.*)/ ) {
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

sub load {
    my ($pkg, $id) = @_;
}

sub vol_unset {
    my ($self, $fld) = @_;
    delete $self->[VOL]{$fld};
}

sub vol_get {
    my ($self, $fld, $val) = @_;
    if (! defined $self->[VOL]{$fld}) {
        $self->[VOL]{$fld} = $val;
    }
    return $self->[VOL]{$fld};
}

sub vol_set {
    my ($self, $fld, $val) = @_;
    $self->[VOL]{$fld} = $val;
}

1;
