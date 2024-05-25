package Yote::SQLObjectStore::LookupHash;

use 5.16.0;
use warnings;

use base 'Yote::SQLObjectStore::BaseStorable';

sub ready {
    my ($pkg, $store, $id, $handle) = @_;
    my( $key_size, $value_type ) = ( $handle =~ /^\^HASH<(\d+)>_(.*)/ );

    my %args = (
        ID => $id,

        data           => {},
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
    my ($self, $key, $value) = @_;

    my $data = $self->data;
    my $inval = $self->store->xform_in( $value, $self->{value_type} );

    no warnings 'uninitialized';

    $self->dirty;
    $data->{$key} = $inval;
    delete $self->{deletes}{$key};

    return $value;
}

sub get {
    my ($self, $key) = @_;

    my ($val) = $self->slice( $key );

    return $val;
}

sub slice {
    my ($self, @keys) = @_;

    return () unless @keys;

    my $data = $self->data;

    my %slice;
    if ($self->{clear_all}) {
        # cleared, but may have data set after the clear
        (%slice) = map { $_ => $data->{$_} } keys %$data;
    } 
    else {
        my $value_type = $self->{value_type};
        my $store = $self->store;
        my $table = $self->table;
        
        my $sql = "SELECT hashkey,val FROM $table WHERE id=? AND (". join(' OR ', ('hashkey=?') x @keys).")";
        
        $store->apply_query_array( $sql,
                                   [$self->id, @keys],
                                   sub  {
                                       my ($k, $v) = @_;
                                       # if the key exists in the data, it may mean that there is an override here
                                       $slice{$k} = $store->xform_out( $v, $value_type );

                                   } );
    }

    my $deletes = $self->{deletes};

    my @ret;
    
    for my $key (@keys) {
        if ($deletes->{$key}) {
            push @ret, undef;
        } else {
            push @ret, $slice{$key};
        }
    }

    return @ret;
}

sub clear {
    my ($self) = @_;
    my $data = $self->data;
    %$data = ();
    $self->dirty;
    $self->{clear_all} = 1;
    $self->{deletes} = {};
}

sub delete_key {
    my ($self, $key) = @_;
    my $data = $self->data;
    return undef unless exists $data->{$key} && defined $data->{$key};

    my $val = $data->{$key};


    $data->{$key} = undef; #set as undef so it will be delete in the db
    $self->dirty;
    return $self->store->xform_out( $val, $self->{value_type} );
}

sub save_sql {
    my $self = shift;

    my @ret_sql;

    my $id = $self->id;

    my $table = $self->table;

    if ($self->{clear_all}) {
        my $sql = "DELETE FROM $table WHERE id=?";
        push @ret_sql, [ $sql, $id ];
    }

    my $deleted = $self->{deleted};
    my $data = $self->data;
    my (@fields) = keys %$data;

    if (@fields) {
     
        my @insert_qparams;
        my @delete_qparams;

        for my $key (@fields) {
            if ($deleted->{$key}) {
                push @delete_qparams, $key;
            } else {
                push @insert_qparams, [$id, $key, $data->{$key}];
            }
        }
        if (@delete_qparams) {
            my $sql = 
                "DELETE FROM $table WHERE id=? AND (".
                join( ' OR ', ('hashkey=?') x @delete_qparams ) .")";
            push @ret_sql, [ $sql, $id, @delete_qparams ];
            
        }
        if (@insert_qparams) {
            my $store = $self->store;
            my $sql = 
                $store->insert_or_replace." INTO $table (id,hashkey,val) VALUES " 
                .join( ',', ("(?,?,?)") x @insert_qparams);
            push @ret_sql, [$sql, map { @$_ } @insert_qparams];
        }
    }
    return @ret_sql;

}

1;
__END__


