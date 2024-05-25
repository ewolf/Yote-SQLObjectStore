package Yote::SQLObjectStore::Array;

use 5.16.0;
use warnings;

use Yote::SQLObjectStore::TiedArray;

use base 'Yote::SQLObjectStore::BaseStorable';

sub new {
    my ($pkg, %args) = @_;
    my $arry = $pkg->SUPER::new(%args);
    $arry->{value_type} = $args{value_type};
    return $arry;
}

sub get {
    my ($self, $idx) = @_;
#    print STDERR Data::Dumper->Dump([$self->{data},"ARRAY GET $idx"]);
    $self->slice( $idx, 1 )->[0];
}

sub set {
    my ($self, $idx, $val) = @_;
    my $data = $self->data;
    my $inval = $self->store->xform_in( $val, $self->{value_type} );
    no warnings 'uninitialized';
    unless (exists $data->[$idx] && $data->[$idx] eq $inval) {
        $self->dirty;
        $data->[$idx] = $inval;
    }
    return $val;
}

sub push {
    my ($self, @vals) = @_;
    return unless @vals;
    my $store = $self->store;
    my $data = $self->data;
    my $value_type = $self->{value_type};
    my (@invals) = map { $store->xform_in($_, $value_type) } @vals;
    $self->dirty;
    CORE::push @$data, @invals;
    return scalar(@$data);
}

sub pop {
    my $self = CORE::shift;
    my $data = $self->data;
    return unless @$data;

    $self->dirty;
    my $val = pop @$data;
    return $self->store->xform_out( $val, $self->{value_type} );
}

sub shift {
    my $self = CORE::shift;
    my $data = $self->data;
    return unless @$data;

    $self->dirty;
    my $val = CORE::shift @$data;
    return $self->store->xform_out( $val, $self->{value_type} );
}

sub unshift {
    my ($self, @vals) = @_;
    return unless @vals;
    my $store = $self->store;
    my $data = $self->data;
    my $value_type = $self->{value_type};
    my (@invals) = map { $store->xform_in($_, $value_type) } @vals;
    $self->dirty;
    unshift @$data, @invals;
    return scalar(@$data);
}

sub splice {
    my ($self, $pos, $amount, @vals) = @_;

    my $store = $self->store;
    my $data = $self->data;
    my $value_type = $self->{value_type};

    my (@invals) = map { $store->xform_in($_, $value_type) } @vals;
    $self->dirty;
    my @outvals = splice @$data, $pos, $amount, @invals;

    return map { $store->xform_out( $_, $value_type ) } @outvals;
}

sub delete {
    my ($self, $idx) = @_;
    my $data = $self->data;
    return undef unless exists $data->[$idx] && defined $data->[$idx];

    my $val = $data->[$idx];
    $self->dirty;
    delete $data->[$idx];

    return $self->store->xform_out( $val, $self->{value_type} );
}

sub size {
    my $data = CORE::shift->data;
    scalar(@$data)
}

sub slice {
    my ($self, $idx, $length) = @_;

    my $value_type = $self->{value_type};
    my $store = $self->store;
    my $data = $self->data;

    #
    # convert parameters to numbers
    #
    $idx = int $idx;
    no warnings 'uninitialized';
    $length = int $length;
    my $to_idx = $length > 0 ? ($idx+$length-1) : $#$data;
    if ($to_idx > $#$data) {$to_idx = $#$data}

    if ($self->{fully_loaded}) {
        return [map { $store->xform_out( $_, $value_type ) }
                @$data[$idx..$to_idx]];
    }
    
    my $LIMIT = $length > 0 ? " LIMIT $idx, $length" : " LIMIT $idx";

    my $table = $self->table;

    my $slice = [];
    if ($#$data >= $to_idx && $to_idx > 0) {
        for ($idx..$to_idx) {
            CORE::push @$slice, $store->xform_out( $data->[$_], $value_type );
        }
    }
#print STDERR Data::Dumper->Dump([$data,"$idx..$to_idx","$#$data >= $to_idx",$slice,"SLI"]);

    my $sql = "SELECT idx,val FROM $table WHERE id=? $LIMIT";

    $store->apply_query_array( $sql,
                               [$self->id],
                               sub  {
                                   my ($item_idx,$v) = @_;
                                   my $slice_idx = $item_idx - $idx;
#                                   print STDERR Data::Dumper->Dump(["Add item $item_idx, $v to SLICE as slice idx $slice_idx"]);
                                   $slice->[$slice_idx] = $store->xform_out( $v, $value_type );
                               } );
#print STDERR Data::Dumper->Dump([$slice,"REUTRN SLICE"]);
    return $slice;

}

sub clear {
    my ($self) = @_;
    my $data = $self->data;
    if (scalar(@$data)) {
        $self->dirty;
    }
    $data = [ map { $_ => undef } keys %$data ];
}

sub tied_array {
    my $self = CORE::shift;

    # check if this has been loaded from the database
    my $tied_array = $self->{tied_array};
    return $tied_array if $tied_array;

    $self->load_all;

    # load entire hash from db
    $tied_array = $self->{tied_array} = [];

    tie @$tied_array, 'Yote::SQLObjectStore::TiedArray', $self;

    return $tied_array;
}

sub load_all {
    my $self = CORE::shift;

    my $data     = $self->{data};
    my $store    = $self->store;
    my $table    = $self->{table};

    $store->apply_query_array(
        "SELECT idx,val FROM $table WHERE id=?",
        [$self->id],
        sub {
            my ($i, $v) = @_;
            $data->[$i] = $v;
        } );
    $self->{fully_loaded} = 1;
}

sub save_sql {
    my $self = CORE::shift;

    my $data = $self->data;
    my $fields = scalar @$data;
    if ($fields) {
        my $id = $self->id;
        my $table = $self->table;
        return
            [ "DELETE FROM $table WHERE id=?", $id ],
            [ "INSERT INTO $table (id,idx,val) VALUES "
              .join( ',', ("(?,?,?)") x $fields),
              map { $id, $_, $data->[$_] } (0..$#$data)
            ];
    }
    return ();
}

1;
