package Yote::SQLObjectStore::TiedArray;

use 5.16.0;
use warnings;

sub TIEARRAY {
    my( $pkg, $blessed_array ) = @_;
    my $data = $blessed_array->{tied_array};
    my $tied = bless { 
        id           => $blessed_array->id,
        blessed_array => $blessed_array,
        data         => [@$data],
    }, $pkg;

    return $tied;
} #TIEARRAY

sub blessed_array {
    shift->{blessed_array};
}

sub data {
    shift->{data};
}

sub EXTEND {}

sub FETCH {
    my( $self, $idx ) = @_;
    my $data = $self->data;
    return undef if $idx >= @$data;
    return $self->blessed_array->store->xform_out( $data->[$idx], $self->blessed_array->{value_type} );
} #FETCH

sub FETCHSIZE {
    return scalar( @{shift->data} );
}

sub STORE {
    my( $self, $idx, $val ) = @_;

    my $data = $self->data;
    my $oldval = $data->[$idx];
    my $blessed_array = $self->blessed_array;
    my $inval = $blessed_array->store->xform_in( $val, $blessed_array->{value_type} );
    ( $inval ne $oldval ) && $blessed_array->dirty;
    $blessed_array->data->[$idx] = $data->[$idx] = $inval;
    return $val;
} #STORE

sub STORESIZE {
    my( $self, $size ) = @_;

    my $data = $self->data;
    $#$data = $size - 1;

} #STORESIZE

sub EXISTS {
    my( $self, $idx ) = @_;
    return exists $self->data->[$idx];
} #EXISTS

sub DELETE {
    my( $self, $idx ) = @_;
    my $data = $self->data;
    my $blessed_array = $self->blessed_array;
    (exists $data->[$idx]) && $blessed_array->dirty;
    my $val = delete $data->[$idx];
    delete $blessed_array->data->[$idx];
    return $self->blessed_array->store->xform_out( $val, $blessed_array->{value_type} );
} #DELETE

sub CLEAR {
    my $self = shift;
    my $data = $self->data;
    @$data && $self->blessed_array->dirty;
    @{$self->blessed_array->data} = ();
    @$data = ();
}
sub PUSH {
    my( $self, @vals ) = @_;
    my $data = $self->data;
    my $blessed_array = $self->blessed_array;
    if (@vals) {
        $blessed_array->dirty;
    }
    my $store = $blessed_array->store;
    my $value_type = $blessed_array->{value_type};
    my @topush = map { $store->xform_in($_, $value_type) } @vals;
    push @{$blessed_array->data}, @topush;
    my $ret = push @$data, @topush;
        
    return $ret;
}
sub POP {
    my $self = shift;
    my $item = pop @{$self->data};
    my $blessed_array = $self->blessed_array;
    $blessed_array->dirty;
    pop @{$blessed_array->data};
    return $blessed_array->store->xform_out( $item, $blessed_array->{value_type} );
}
sub SHIFT {
    my( $self ) = @_;
    my $item = shift @{$self->data};
    my $blessed_array = $self->blessed_array;
    $blessed_array->dirty;
    shift @{$blessed_array->data};
    return $blessed_array->store->xform_out( $item, $blessed_array->{value_type} );
}

sub UNSHIFT {
    my( $self, @vals ) = @_;
    my $data = $self->data;
    my $blessed_array = $self->blessed_array;
    my $store = $blessed_array->store;
    @vals && $blessed_array->dirty;
    my $value_type = $blessed_array->{value_type};
    my @to_fill = map { $store->xform_in($_,$value_type) } @vals;
    unshift @{$blessed_array->data}, @to_fill;
    return unshift @$data, @to_fill;
	
}

sub SPLICE {
    my( $self, $offset, $remove_length, @vals ) = @_;
    my $data = $self->data;
    my $blessed_array = $self->blessed_array;
    my $store = $blessed_array->store;
    $blessed_array->dirty;
    my $value_type = $blessed_array->{value_type};
    my @to_splice = map { $store->xform_in($_, $value_type) } @vals;
    splice @{$blessed_array->data}, $offset, $remove_length, @to_splice;
    return map { $store->xform_out($_, $value_type) } splice @$data, $offset, $remove_length, @to_splice;
	
} #SPLICE

1;
