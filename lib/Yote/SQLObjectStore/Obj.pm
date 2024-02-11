package Yote::SQLObjectStore::Obj;

use 5.14.0;

no warnings 'uninitialized';

use MIME::Base64;

use constant {
    ID	     => 0,
    DATA     => 1,
    STORE    => 2,
    VOL      => 3,
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
# Stub methods to override
#
sub _init {}
sub _load {}

#
# private stuff
#
sub __data {
    return shift->[DATA];
}

#
# Instance methods
#
sub id {
    return shift->[ID];
}

sub store {
    return shift->[STORE];
}

sub fields {
    return [keys %{shift->[DATA]}];
}

sub get {
    my ($self,$field,$default) = @_;
    if ((! exists $self->[DATA]{$field}) and defined($default)) {
	return $self->set($field,$default);
    }
    return $self->[STORE]->_xform_out( $self->[DATA]{$field} );
} #get

sub set {
    my ($self,$field,$value) = @_;
    my $inval = $self->[STORE]->_xform_in($value);
    my $dirty = $self->[DATA]{$field} ne $inval;
    $self->[DATA]{$field} = $inval;
    $dirty && $self->_dirty;
    return $value;
} #set

sub _dirty {
    my $self = shift;
    $self->[STORE]->_dirty( $self->[ID], $self );
}

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

sub __logline {
    # produces a string : $id $classname p1 p2 p3 ....
    # where the p parts are quoted if needed 

    my $self = shift;

    my $data = $self->__data;    
    my (@data) = (map { my $v = $data->{$_};
                        $_ => $v =~ /[\s\n\r]/g ? '"' . MIME::Base64::encode( $v, '' ).'"' : $v
                      }
                  keys %$data);
    return join( " ", $self->id, ref( $self ), @data );
}

sub __freezedry {
    # packs into
    #  I - length of package name (c)
    #  a(c) - package name
    #  L - object id
    #  I - number of components (n)
    #  I(n) lenghts of components
    #  a(sigma n) data 
    my $self = shift;

    my $r = ref( $self );
    my $c = length( $r );
    
    my $data = $self->__data;

    my (@data) = (map { $_ => $data->{$_} } keys %$data);
    my $n = scalar( @data );

    my (@lengths) = map { do { use bytes; length($_) } } @data;

    my $pack_template = "I(a$c)LI(I$n)" . join( '', map { "(a$_)" } @lengths);

    return pack $pack_template, $c, $r, $self->id, $n, @lengths, @data;
}

sub __reconstitute {
    my ($self, $id, $data, $store, $update_time, $creation_time ) = @_;

    my $unpack_template = "I";

    my $class_length = unpack $unpack_template, $data;

    $unpack_template .= "(a$class_length)LI";
    my (undef, $class, $stored_id, $n) = unpack $unpack_template, $data;

    $unpack_template .= "I" x $n;

    my (undef, undef, undef, undef, @lengths) = unpack $unpack_template, $data;

    $unpack_template .= join( '', map { "(a$_)" } @lengths );
    
    my( @parts ) = unpack $unpack_template, $data;

    # remove beginning stuff
    splice @parts, 0, ($n+4);

    if( $class ne 'Yote::ObjectStore::Obj' ) {
      my $clname = $class;
      $clname =~ s/::/\//g;

      require "$clname.pm";
    }

    my $obj = bless [
        $id,
        {@parts},
        $store,
        {}
        ], $class;
    # stuff into WEAK temporarily while LOAD happens
    $store->_weak($id,$obj);
    $obj->_load;

    return $obj;

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

"CONTAIN";
