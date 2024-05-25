package Yote::SQLObjectStore::LookupArray;

use 5.16.0;
use warnings;

use 5.16.0;
use warnings;

use base 'Yote::SQLObjectStore::BaseStorable';

sub ready {
    my ($pkg, $store, $id, $handle) = @_;
    my( $key_size, $value_type ) = ( $handle =~ /^\^HASH<(\d+)>_(.*)/ );
    my %args = (
        ID => $id,

        data           => {}, # yes, using a hash
        key_size       => $key_size,
        has_first_save => 1,
        store          => $store,
        table          => $store->get_table_manager->label_to_table($handle),
        type           => $handle,
        value_type     => $value_type,

        );
    my $self = $pkg->SUPER::new(%args);

    # hash deleted keys here
    $self->{deletes} = {};

    return $self;
}

sub set {
    my ($self, $idx, $val) = @_;

    my $data = $self->data;
    my $inval = $self->store->xform_in( $val, $self->{value_type} );

    no warnings 'uninitialized';

    if ($idx >= $self->{size}) {
        $self->{size} += (1 + $idx - $self->{size});
    }

    unless (exists $data->{$idx} && $data->{$idx} eq $inval) {
        $self->dirty;
        $data->{$idx} = $inval;
    }
    delete $self->{deletes}{$idx};

    return $val;
}

sub get {
    my ($self, $idx) = @_;

    my ($val) = $self->slice( $idx, 1 );

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
    $self->{size} += @vals;
    return scalar(@$data);
}

sub pop {
    my $self = CORE::shift;
    my $data = $self->data;
    return unless $self->{size};

    $self->dirty;
    my $val = pop @$data;
    $self->{size}--;
    return $self->store->xform_out( $val, $self->{value_type} );
}

sub shift {
    my $self = CORE::shift;
    my $data = $self->data;
    return unless $self->{size};

    $self->dirty;
    my $val = CORE::shift @$data;
    $self->{size}--;
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
    $self->{size} += @vals;
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
    $self->{size} += (@invals - @outvals);

    return map { $store->xform_out( $_, $value_type ) } @outvals;
}

sub delete {
    my ($self, $idx) = @_;
    my $data = $self->data;
    return undef unless exists $data->[$idx] && defined $data->[$idx];

    my $val = $data->[$idx];
    $self->dirty;
    delete $data->[$idx];
    if ($idx == $self->{size} - 1) {
        $self->{size}--;
    }

    return $self->store->xform_out( $val, $self->{value_type} );
}

sub size {
    CORE::shift->{size};
}

sub slice {
    my ($self, $idx, $length) = @_;

    return () if $length == 0;

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

    my $sql = "SELECT idx,val FROM $table WHERE id=? $LIMIT";
    $store->apply_query_array( $sql,
                               [$self->id],
                               sub  {
                                   my ($item_idx,$v) = @_;
                                   my $slice_idx = $item_idx - $idx;
                                   $slice->[$slice_idx] = $store->xform_out( $v, $value_type );
                               } );
    if ($#$data >= $to_idx && $to_idx > 0) {
        for ($idx..$to_idx) {
            CORE::push @$slice, $store->xform_out( $data->[$_], $value_type );
        }
    }

    return $slice;

}

sub clear {
    my ($self) = @_;
    my $data = $self->data;
    if ($self->{size} > 0) {
        $self->{size} = 0;
        $self->dirty;
    }
    $data = [];
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
    my $entries = scalar @$data;
    if ($entries) {
        my $id = $self->id;
        my $table = $self->table;
        my @sql;
        if ($self->{size} == @$data) {
            CORE::push @sql, [ "DELETE FROM $table WHERE id=?", $id ];
        }
        if ($self->{size}) {
            CORE::push @sql, [ $self->store->insert_or_replace . " INTO $table (id,idx,val) VALUES "
                               .join( ',', ("(?,?,?)") x $entries),
                               map { $id, $_, $data->[$_] } (0..$#$data)];
        }
        return @sql;
    }
    return ();
}

sub load_info {
    my $self = CORE::shift;
    my $table = $self->table;
    my $sql = "SELECT count(*) FROM $table WHERE id=?";
    my ($db_size) = $self->store->query_line( $sql, $self->id );
    my $data_size = scalar(@{$self->{data}});

 $sql = "SELECT max(idx) FROM $table WHERE id=?";
    my ($max) = $self->store->query_line( $sql, $self->id );

    $self->{size} = $db_size > $data_size ? $db_size : $data_size;
}

1;
