package Yote::SQLObjectStore::Hash;

use 5.16.0;
use warnings;

use Yote::SQLObjectStore::TiedHash;

use base 'Yote::SQLObjectStore::BaseStorable';

sub new {
    my ($pkg, %args) = @_;

    my $hash = $pkg->SUPER::new( %args );
    for my $fld (qw(value_type key_size)) {
        $hash->{$fld} = $args{$fld};
    }
    return $hash;
}

sub lookup {
    my ($self, $key) = @_;
    $self->slice( $key )->{$key};
}

sub slice {
    my ($self, @keys) = @_;

    return {} unless @keys;

    my $value_type = $self->{value_type};
    my $store = $self->store;
    if (my $hash_ref = $self->{hash_ref}) {
        return { map { $_ => $store->xform_out( $hash_ref->{$_}, $value_type ) } @keys };
    }

    my $data = $self->data;
    if (!$self->has_first_save) {
        # if this has not had its first save, use the data hash rather than the table
        return { map { $_ => $store->xform_out( $data->{$_}, $value_type ) } @keys };
    }

    my $table = $self->table;

    my $slice = {};
    my $sql = "SELECT val FROM $table WHERE id=? AND (". join(' OR ', ('key=?') x @keys).")";

    $store->apply_query_array( $sql,
                               [$self->id, @keys],
                               sub  {
                                   my ($k, $v) = @_;
                                   # if the key exists in the data, it may mean that there is an override here
                                   $slice->{$k} = $store->xform_out( exists $data->{$k} ? $data->{$k} : $v, $value_type );
                               } );
    return $slice;
}

sub set {
    my ($self, $key, $value) = @_;
    my $data = $self->data;
    my $inval = $self->store->xform_in( $value, $self->{value_type} );
    unless (exists $data->{$key} && $data->{$key} ne $inval) {
        $self->dirty;
        $data->{$key} = $inval;
        my $hash_ref = $self->{hash_ref};
        $hash_ref && ($hash_ref->{$key} = $value);
    }
    return $value;
}

sub delete_key {
    my ($self, $key) = @_;
    my $data = $self->data;
    return undef unless exists $data->{$key};
    
    my $val = $data->{$key};
    $data->{$key} = undef; #set as undef so it will be delete in the db
    $self->dirty;
    return $self->store->xform_out( $val, $self->{value_type} );
}

sub tied_hash {
    my $self = shift;
    # load in hash ref from database
    my $hash_ref = $self->{hash_ref};
    return $hash_ref if $hash_ref;

    $hash_ref = $self->{hash_ref} = {};

    my $data     = $self->{data};
    my $val_type = $self->{value_type};
    my $store    = $self->store;
    my $table    = $self->{table};

    if (!$self->has_first_save) {
        for my $key (keys %$data) {
            $hash_ref->{$key} = $data->{$key};
        }
    }
    else {
        # load entire hash from db

        $store->apply_query_array(
            "SELECT key,val FROM $table WHERE id=?",
            [$self->id],
            sub {
                my ($k, $v) = @_;
                $data->{$k} = $v;
                $hash_ref->{$k} = $store->xform_out( $v, $val_type );
            } );
    }
    
    tie %$hash_ref, 'Yote::SQLObjectStore::TiedHash', $self;

    return $hash_ref;
}

sub save_sql {
    my $self = shift;

    my $data = $self->data;
    my $fields = scalar keys %$data;

    my @ret_sql;

    if ($fields) {
        my $id = $self->id;
        my $table = $self->table;
     
        my @insert_qparams;
        my @delete_qparams;

        for my $key (keys %$data) {
            my $val = $data->{$key};
            if (defined $val) {
                push @insert_qparams, [$id, $key, $val];
            } else {
                push @delete_qparams, $key;
            }
        }
        if (@delete_qparams) {
            my $sql = 
                "DELETE FROM $table WHERE id=? AND (".
                join( ' OR ', ('key=?') x @delete_qparams ) .")";
            push @ret_sql, [ $sql, $id, @delete_qparams ];
            
        }
        if (@insert_qparams) {
            my $sql = 
                "INSERT INTO $table (id,key,val) VALUES " 
                .join( ',', ("(?,?,?)") x @insert_qparams);
            push @ret_sql, [$sql, map { @$_ } @insert_qparams];
        }
    }
    return @ret_sql;

}

1;
__END__


