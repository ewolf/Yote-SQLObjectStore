package Yote::SQLObjectStore::ReadOnlyHash;

use 5.16.0;
use warnings;

sub TIEHASH {
    my( $pkg, $id, $store, %hash ) = @_;
    my $tied = bless { 
        id    => $id,
        store => $store,
        data  => { %hash }
    }, $pkg;
print STDERR Data::Dumper->Dump([$tied,"PIE"]);
    return $tied;
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

1;
