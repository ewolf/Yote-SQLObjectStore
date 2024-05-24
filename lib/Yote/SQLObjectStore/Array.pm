package Yote::SQLObjectStore::Array;

use 5.16.0;
use warnings;

use Yote::SQLObjectStore::TiedArray;

use base 'Yote::SQLObjectStore::BaseStorable';

sub new {
    my ($pkg, %args) = @_;

    my $array = $pkg->SUPER::new( %args );
    $array->{value_type} = $args{value_type};
    return $array;
}

sub get {
    my ($self, $idx) = @_;
    $self->slice( $idx, 1 )->[0];
}

sub slice {
    my ($self, $idx, $length) = @_;

    # check if there is a tied array
    if (my $tied_array = $self->{tied_array}) {
        no warnings 'uninitialized';
        my $to_idx = $length > 0 ? ($idx+$length-1) : $#$tied_array;
        if ($to_idx > $#$tied_array ) { $to_idx = $#$tied_array }
        return [@$tied_array[$idx..$to_idx]];
    }
    

    # convert to numbers
    $idx = int $idx;
    $length = int $length;
    my $LIMIT = $length > 0 ? " LIMIT $idx, $length" : " LIMIT $idx";

    my $value_type = $self->{value_type};
    my $store = $self->store;

    my $data = $self->data;
    if (!$self->has_first_save) {
        # if this has not had its first save, use the data hash rather than the table
        my $to_idx = $length > 0 ? ($idx+$length-1) : $#$data;
        if ($to_idx > $#$data ) { $to_idx = $#$data }
        return [ map { $store->xform_out( $_, $value_type ) } @$data[$idx..$to_idx] ];
    }

    my $table = $self->table;

    my $slice = [];
    my $sql = "SELECT val FROM $table WHERE id=? $LIMIT";

    $store->apply_query_array( $sql,
                               [$self->id],
                               sub  {
                                   my ($v) = @_;
                                   push @$slice, $store->xform_out( $v, $value_type );
                               } );
    return $slice;

}

sub tied_array {
    my $self = shift;

    # check if this has been loaded from the database
    my $tied_array = $self->{tied_array};
    return $tied_array if $tied_array;

    # load entire hash from db
    $tied_array = $self->{tied_array} = [];

    my $data     = $self->{data};
    my $val_type = $self->{value_type};
    my $store    = $self->store;
    my $table    = $self->{table};

    if (!$self->has_first_save) {
        for my $idx (0..$#$data) {
            $tied_array->[$idx] = $data->[$idx];
        }
    } else {
        $store->apply_query_array(
            "SELECT idx,val FROM $table WHERE id=?",
            [$self->id],
            sub {
                my ($idx, $v) = @_;
                $data->[$idx] = $tied_array->[$idx] = $v;
            }
            );
    }
    
    tie @$tied_array, 'Yote::SQLObjectStore::TiedArray', $self;

    return $tied_array;
}

sub save_sql {
    my $self = shift;

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
