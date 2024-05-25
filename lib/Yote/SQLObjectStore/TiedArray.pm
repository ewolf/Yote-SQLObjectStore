package Yote::SQLObjectStore::TiedArray;

use 5.16.0;
use warnings;

sub TIEARRAY {
    my( $pkg, $blessed_array ) = @_;
    my $tied = bless { 
        id           => $blessed_array->id,
        blessed_array => $blessed_array,
    }, $pkg;

    return $tied;
} #TIEARRAY

sub blessed_array {
    shift->{blessed_array};
}

sub EXTEND {}

sub FETCH {
    my( $self, $idx ) = @_;
    $self->blessed_array->get($idx);
} #FETCH

sub FETCHSIZE {
    shift->blessed_array->size;
}

sub STORE {
    my( $self, $idx, $val ) = @_;
    $self->blessed_array->set( $idx, $val );
} #STORE

sub STORESIZE {
    my( $self, $size ) = @_;

    my $data = $self->blessed_array->data;
    $#$data = $size - 1;

} #STORESIZE

sub EXISTS {
    my( $self, $idx ) = @_;
    return exists $self->blessed_array->data->[$idx];
} #EXISTS

sub DELETE {
    my( $self, $idx ) = @_;
    $self->blessed_array->delete($idx);
} #DELETE

sub CLEAR {
    my $self = shift;
    my $data = $self->blessed_array->data;
    @$data && $self->blessed_array->dirty;
    @{$self->blessed_array->data} = ();
    @$data = ();
}
sub PUSH {
    my( $self, @vals ) = @_;
    $self->blessed_array->push(@vals);
}
sub POP {
    shift->blessed_array->pop;
}
sub SHIFT {
    shift->blessed_array->shift;
}

sub UNSHIFT {
    my( $self, @vals ) = @_;
    $self->blessed_array->unshift(@vals);
}

sub SPLICE {
    my( $self, $offset, $remove_length, @vals ) = @_;
    $self->blessed_array->splice($offset,$remove_length, @vals);
} #SPLICE

1;
