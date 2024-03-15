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

sub tied_array {
    my $self = shift;

    # check if this has been loaded from the database
    my $array_ref = $self->{array_ref};
    return $array_ref if $array_ref;

    # load entire hash from db
    $array_ref = $self->{array_ref} = [];

    my $data     = $self->{data};
    my $val_type = $self->{value_type};
    my $store    = $self->store;
    my $table    = $self->{table};

    if (!$self->has_first_save) {
        for my $idx (0..$#$data) {
            $array_ref->[$idx] = $data->[$idx];
        }
    } else {
        $store->apply_query_array(
            "SELECT idx,val FROM $table WHERE id=?",
            [$self->id],
            sub {
                my ($idx, $v) = @_;
                $data->[$idx] = $array_ref->[$idx] = $v;
            }
            );
    }
    
    tie @$array_ref, 'Yote::SQLObjectStore::TiedArray', $self;

    return $array_ref;
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
