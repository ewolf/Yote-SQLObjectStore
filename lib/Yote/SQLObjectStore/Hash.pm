package Yote::ObjectStore::Hash;

use 5.14.0;

no warnings 'uninitialized';

use MIME::Base64;
use Tie::Hash;

use constant {
    ID         => 0,
    DATA       => 1,
    OBJ_STORE  => 2,
    META       => 3,
};

sub _dirty {
    my $self = shift;
    $self->[OBJ_STORE]->_dirty( $self->[ID], $self );
} #_dirty

sub __data {
    return shift->[DATA];
}

sub id {
    return shift->[ID];
}

sub TIEHASH {
    my( $pkg, $id, $store, $meta, %hash ) = @_;
    return bless [ $id,
		   { %hash },
		   $store,
		   $meta,
	], $pkg;
} #TIEHASH

sub CLEAR {
    my $self = shift;
    my $data = $self->[DATA];
    $self->_dirty if scalar( keys %$data );
    %$data = ();
} #CLEAR

sub DELETE {
    my( $self, $key ) = @_;

    my $data = $self->[DATA];
    return undef unless exists $data->{$key};
    $self->_dirty;
    my $oldval = delete $data->{$key};
    return $self->[OBJ_STORE]->_xform_out( $oldval );
} #DELETE


sub EXISTS {
    my( $self, $key ) = @_;
    return exists $self->[DATA]{$key};
} #EXISTS

sub FETCH {
    my( $self, $key ) = @_;
    return $self->[OBJ_STORE]->_xform_out( $self->[DATA]{$key} );
} #FETCH

sub STORE {
    my( $self, $key, $val ) = @_;
    my $data = $self->[DATA];
    my $oldval = $data->{$key};
    my $inval = $self->[OBJ_STORE]->_xform_in( $val );
    ( $inval ne $oldval ) && $self->_dirty;
    $data->{$key} = $inval;
    return $val;
} #STORE

sub FIRSTKEY {
    my $self = shift;

    my $data = $self->[DATA];
    my $a = scalar keys %$data; #reset the each
    my( $k, $val ) = each %$data;
    return $k;
} #FIRSTKEY

sub NEXTKEY  {
    my $self = shift;
    my $data = $self->[DATA];
    my( $k, $val ) = each %$data;
    return $k;
} #NEXTKEY

sub __logline {
    # produces a string : $id $classname p1 p2 p3 ....
    # where the p parts are quoted if needed 

    my $self = shift;

    my $data = $self->__data;    
    my (@data) = (map { my $v = $data->{$_};
                        $_ => $v =~ /[\s\n\r]/g ? '"'.MIME::Base64::encode( $v, '' ).'"' : $v
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

    my %hash;
    tie %hash, 'Yote::ObjectStore::Hash', $id, $store, {updated => $update_time, created => $creation_time}, @parts;
    return \%hash;
}

"HASH IT OUT";
