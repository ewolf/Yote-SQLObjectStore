package Yote::SQLObjectStore::BaseArray;

use 5.14.0;

no warnings 'uninitialized';

use MIME::Base64;
use Tie::Array;

use constant {
    ID          => 0,
    TABLE       => 1,
    DATA_TYPE   => 2,
    DATA        => 3,
    OBJ_STORE   => 4,
};

sub dirty {
    my $self = shift;
    $self->[OBJ_STORE]->dirty( $self->[ID], $self );
}

sub table_name {
    return shift->[TABLE];
}

sub data_type {
    return shift->[DATA_TYPE];
}

sub data {
    return shift->[DATA];
}

sub id {
    return shift->[ID];
}

sub EXTEND {}

sub TIEARRAY {
    my( $pkg, $id, $store, $table_name, $data_type, $meta, @list ) = @_;
    
    return bless [
        $id,
        $table_name,
        $data_type,
        [@list],
	$store,
	$meta,
	], $pkg;

} #TIEARRAY

sub FETCH {
    my( $self, $idx ) = @_;

    my $data = $self->[DATA];
    return undef if $idx >= @$data;
    return $self->[OBJ_STORE]->xform_out( $self->[DATA][$idx], $self->[DATA_TYPE] );
    
} #FETCH

sub FETCHSIZE {
    return scalar( @{shift->[DATA]} );
}

sub STORE {
    my( $self, $idx, $val ) = @_;
    my $inval = $self->[OBJ_STORE]->xform_in( $val, $self->[DATA_TYPE] );
    if ($inval ne $self->[DATA][$idx]) {
        $self->dirty;
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
    (exists $self->[DATA]->[$idx]) && $self->dirty;
    my $val = delete $self->[DATA][$idx];
    return $self->[OBJ_STORE]->xform_out( $val, $self->[DATA_TYPE] );
} #DELETE

sub CLEAR {
    my $self = shift;
    my $data = $self->[DATA];
    @$data && $self->dirty;
    @$data = ();
}
sub PUSH {
    my( $self, @vals ) = @_;
    my $data = $self->[DATA];
    if (@vals) {
        $self->dirty;
    }
    my $ret =  push @$data,
        map { $self->[OBJ_STORE]->xform_in($_, $self->[DATA_TYPE]) } @vals;
    return $ret;
}
sub POP {
    my $self = shift;
    my $item = pop @{$self->[DATA]};
    $self->dirty;
    return $self->[OBJ_STORE]->xform_out( $item, $self->[DATA_TYPE] );
}
sub SHIFT {
    my( $self ) = @_;
    my $item = shift @{$self->[DATA]};
    $self->dirty;
    return $self->[OBJ_STORE]->xform_out( $item, $self->[DATA_TYPE] );
}

sub UNSHIFT {
    my( $self, @vals ) = @_;
    my $data = $self->[DATA];
    if (@vals) {
        $self->dirty;
    }
    return unshift @$data,
	map { $self->[OBJ_STORE]->xform_in($_, $self->[DATA_TYPE]) } @vals;
}

sub SPLICE {
    my( $self, $offset, $remove_length, @vals ) = @_;
    my $data = $self->[DATA];
    $self->dirty;
    return map { $self->[OBJ_STORE]->xform_out($_, $self->[DATA_TYPE]) } splice @$data, $offset, $remove_length,
	map { $self->[OBJ_STORE]->xform_in($_, $self->[DATA_TYPE]) } @vals;
} #SPLICE

"ARRAY ARRAY ARRAY";
