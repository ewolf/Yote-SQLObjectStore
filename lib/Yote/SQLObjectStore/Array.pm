package Yote::ObjectStore::Array;

use 5.14.0;

no warnings 'uninitialized';

use MIME::Base64;
use Tie::Array;

use constant {
    ID          => 0,
    DATA        => 1,
    OBJ_STORE   => 2,
};

sub _dirty {
    my $self = shift;
    $self->[OBJ_STORE]->_dirty( $self->[ID], $self );
}

sub __data {
    return shift->[DATA];
}

sub id {
    return shift->[ID];
}

sub EXTEND {}

sub TIEARRAY {
    my( $pkg, $id, $store, $meta, @list ) = @_;
    
    return bless [
        $id,
        [@list],
	$store,
	$meta,
	], $pkg;

} #TIEARRAY

sub FETCH {
    my( $self, $idx ) = @_;

    my $data = $self->[DATA];
    return undef if $idx >= @$data;
    return $self->[OBJ_STORE]->_xform_out( $self->[DATA][$idx] );
    
} #FETCH

sub FETCHSIZE {
    return scalar( @{shift->[DATA]} );
}

sub STORE {
    my( $self, $idx, $val ) = @_;
    my $inval = $self->[OBJ_STORE]->_xform_in( $val );
    if ($inval ne $self->[DATA][$idx]) {
        $self->_dirty;
    }
    $self->[DATA][$idx] = $inval;
    return $val;
} #STORE

sub STORESIZE {
    my( $self, $size ) = @_;

    my $data = $self->[DATA];
    $#$data = $size - 1;

} #STORESIZE

sub EXISTS {
    my( $self, $idx ) = @_;
    return exists $self->[DATA][$idx];
} #EXISTS

sub DELETE {
    my( $self, $idx ) = @_;
    (exists $self->[DATA]->[$idx]) && $self->_dirty;
    my $val = delete $self->[DATA][$idx];
    return $self->[OBJ_STORE]->_xform_out( $val );
} #DELETE

sub CLEAR {
    my $self = shift;
    my $data = $self->[DATA];
    @$data && $self->_dirty;
    @$data = ();
}
sub PUSH {
    my( $self, @vals ) = @_;
    my $data = $self->[DATA];
    if (@vals) {
        $self->_dirty;
    }
    my $ret =  push @$data,
        map { $self->[OBJ_STORE]->_xform_in($_) } @vals;
    return $ret;
}
sub POP {
    my $self = shift;
    my $item = pop @{$self->[DATA]};
    $self->_dirty;
    return $self->[OBJ_STORE]->_xform_out( $item );
}
sub SHIFT {
    my( $self ) = @_;
    my $item = shift @{$self->[DATA]};
    $self->_dirty;
    return $self->[OBJ_STORE]->_xform_out( $item );
}

sub UNSHIFT {
    my( $self, @vals ) = @_;
    my $data = $self->[DATA];
    @vals && $self->_dirty;
    return unshift @$data,
	map { $self->[OBJ_STORE]->_xform_in($_) } @vals;
}

sub SPLICE {
    my( $self, $offset, $remove_length, @vals ) = @_;
    my $data = $self->[DATA];
    $self->_dirty;
    return map { $self->[OBJ_STORE]->_xform_out($_) } splice @$data, $offset, $remove_length,
	map { $self->[OBJ_STORE]->_xform_in($_) } @vals;
} #SPLICE

sub __logline {
    # produces a string : $id $classname p1 p2 p3 ....
    # where the p parts are quoted if needed 

    my $self = shift;
    
    my (@data) = ( map { $_ =~ /[\s\n\r]/g ? '"'.MIME::Base64::encode( $_, '' ).'"' : $_ }
                   map { defined($_) ? $_ : 'u' } 
                   @{$self->__data});
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
    
    my (@data) = (map { defined($_) ? $_ : 'u' } @{$self->__data});
    my $n = scalar( @data );

    my (@lengths) = map { do { use bytes; length($_) } } @data;

    my $pack_template = "I(a$c)LI(I$n)" . join( '', map { "(a$_)" } @lengths);

    return pack $pack_template, $c, $r, $self->id, $n, @lengths, @data;
}

sub __reconstitute {
    my ($self, $id, $data, $store, $update_time, $creation_time ) = @_;

    my $unpack_template = "I";
    my $c = unpack $unpack_template, $data;

    $unpack_template .= "(a$c)";
    (undef, my $class) = unpack $unpack_template, $data;

    $unpack_template .= "LI";
    (undef, undef, undef, my $n) = unpack $unpack_template, $data;
    $unpack_template .= "I" x $n;
    (undef, undef, undef, undef, my @sizes) = unpack $unpack_template, $data;

    $unpack_template .= join( "", map { "(a$_)" } @sizes );

    my( @parts ) = unpack $unpack_template, $data;

    # remove beginning stuff
    splice @parts, 0, ($n+4);

    my @array;
    tie @array, 'Yote::ObjectStore::Array', $id, $store, 
        {updated => $update_time, created => $creation_time}, @parts;
    return \@array;
}

"ARRAY ARRAY ARRAY";
