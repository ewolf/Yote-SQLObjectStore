package Yote::SQLObjectStore::TiedHash;

use 5.16.0;
use warnings;

use Tie::Hash;

sub TIEHASH {
    my( $pkg, $blessed_hash ) = @_;
    my $tied = bless { 
        id           => $blessed_hash->id,
        blessed_hash => $blessed_hash,
    }, $pkg;

    return $tied;
} #TIEHASH

sub blessed_hash {
    shift->{blessed_hash};
}

sub CLEAR {
    my $self = shift;
    $self->blessed_hash->clear;
} #CLEAR

sub DELETE {
    my( $self, $key ) = @_;
    $self->blessed_hash->delete_key($key);
} #DELETE


sub EXISTS {
    my( $self, $key ) = @_;
    return exists $self->blessed_hash->data->{$key};
} #EXISTS

sub FETCH {
    my( $self, $key ) = @_;
    $self->blessed_hash->get($key);
} #FETCH

sub STORE {
    my( $self, $key, $val ) = @_;
    $self->blessed_hash->set($key,$val);
} #STORE

sub FIRSTKEY {
    my $self = shift;

    my $data = $self->blessed_hash->data;
    my $a = scalar keys %$data; #reset the each
    my( $k, $val ) = each %$data;
    return $k;
} #FIRSTKEY

sub NEXTKEY  {
    my $self = shift;
    my $data = $self->blessed_hash->data;
    my( $k, $val ) = each %$data;
    return $k;
} #NEXTKEY

1;
