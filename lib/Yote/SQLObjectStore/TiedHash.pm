package Yote::SQLObjectStore::TiedHash;

use 5.16.0;
use warnings;

sub TIEHASH {
    my( $pkg, $blessed_hash ) = @_;
    my $data = $blessed_hash->{hash_ref};
    my $tied = bless { 
        id           => $blessed_hash->id,
        blessed_hash => $blessed_hash,
        data         => { %$data }
    }, $pkg;

    return $tied;
} #TIEHASH

sub blessed_hash {
    shift->{blessed_hash};
}

sub data {
    shift->{data};
}

sub CLEAR {
    my $self = shift;
    my $data = $self->data;
    $self->blessed_hash->dirty if scalar( keys %$data );
    %$data = ();
} #CLEAR

sub DELETE {
    my( $self, $key ) = @_;

    my $data = $self->data;
    return undef unless exists $data->{$key};
    my $blessed_hash = $self->blessed_hash;
    $blessed_hash->dirty;
    my $oldval = delete $data->{$key};
    return $blessed_hash->store->xform_out( $oldval, $blessed_hash->{value_type} );
} #DELETE


sub EXISTS {
    my( $self, $key ) = @_;
    return exists $self->data->{$key};
} #EXISTS

sub FETCH {
    my( $self, $key ) = @_;
    return $self->blessed_hash->store->xform_out( $self->data->{$key}, $self->blessed_hash->{value_type} );
} #FETCH

sub STORE {
    my( $self, $key, $val ) = @_;
    my $data = $self->data;
    my $oldval = $data->{$key};
    my $blessed_hash = $self->blessed_hash;
    my $inval = $blessed_hash->store->xform_in( $val, $blessed_hash->{value_type} );
    no warnings 'uninitialized';
    ( $inval ne $oldval ) && $blessed_hash->dirty;
    $data->{$key} = $inval;
    return $val;
} #STORE

sub FIRSTKEY {
    my $self = shift;

    my $data = $self->data;
    my $a = scalar keys %$data; #reset the each
    my( $k, $val ) = each %$data;
    return $k;
} #FIRSTKEY

sub NEXTKEY  {
    my $self = shift;
    my $data = $self->data;
    my( $k, $val ) = each %$data;
    return $k;
} #NEXTKEY

1;
