package Yote::SQLObjectStore::BaseHash;

use 5.16.0;
use warnings;
no warnings 'uninitialized';

use Tie::Hash;

use constant {
    ID         => 0,
    TABLE      => 1,
    DATA_TYPE  => 2,
    DATA       => 3,
    OBJ_STORE  => 4,
    META       => 5,
};

sub dirty {
    my $self = shift;
    $self->[OBJ_STORE]->dirty( $self->[ID], $self );
} #dirty

sub table_name {
    return shift->[TABLE];
}

sub data {
    return shift->[DATA];
}

sub id {
    return shift->[ID];
}

sub TIEHASH {
    my( $pkg, $id, $store, $table_name, $data_type, $meta, %hash ) = @_;
    return bless [ $id,
                   $table_name,
                   $data_type,
		   { %hash },
		   $store,
		   $meta,
	], $pkg;
} #TIEHASH

sub CLEAR {
    my $self = shift;
    my $data = $self->[DATA];
    $self->dirty if scalar( keys %$data );
    %$data = ();
} #CLEAR

sub DELETE {
    my( $self, $key ) = @_;

    my $data = $self->[DATA];
    return undef unless exists $data->{$key};
    $self->dirty;
    my $oldval = delete $data->{$key};
    return $self->[OBJ_STORE]->xform_out( $oldval, $self->[DATA_TYPE] );
} #DELETE


sub EXISTS {
    my( $self, $key ) = @_;
    return exists $self->[DATA]{$key};
} #EXISTS

sub FETCH {
    my( $self, $key ) = @_;
    return $self->[OBJ_STORE]->xform_out( $self->[DATA]{$key}, $self->[DATA_TYPE] );
} #FETCH

sub STORE {
    my( $self, $key, $val ) = @_;
    my $data = $self->[DATA];
    my $oldval = $data->{$key};
    my $inval = $self->[OBJ_STORE]->xform_in( $val, $self->[DATA_TYPE] );
    ( $inval ne $oldval ) && $self->dirty;
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


"HASH IT OUT";
