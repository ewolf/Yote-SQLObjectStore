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

sub get {
    my ($self, $key) = @_;
    my $slice = $self->slice( $key );

    $slice->{$key};
}

sub slice {
    my ($self, @keys) = @_;

    return {} unless @keys;

    my $value_type = $self->{value_type};
    my $store = $self->store;

    if (my $data = $self->{data}) {
#print STDERR Data::Dumper->Dump([ { map { $_ => $store->xform_out( $data->{$_}, $value_type ) } @keys }, "SLUICER"]);
        return { map { $_ => $store->xform_out( $data->{$_}, $value_type ) } @keys };
    }

    my $data = $self->data;
    if (!$self->has_first_save) {
        # if this has not had its first save, use the data hash rather than the table
#print STDERR Data::Dumper->Dump([{ map { $_ => $store->xform_out( $data->{$_}, $value_type ) } @keys },"SLUICER2"]);
        return { map { $_ => $store->xform_out( $data->{$_}, $value_type ) } @keys };
    }

    my $table = $self->table;

    my $slice = {};
    my $sql = "SELECT hashkey,val FROM $table WHERE id=? AND (". join(' OR ', ('hashkey=?') x @keys).")";

    $store->apply_query_array( $sql,
                               [$self->id, @keys],
                               sub  {
                                   my ($k, $v) = @_;
                                   # if the key exists in the data, it may mean that there is an override here
                                   $slice->{$k} = $store->xform_out( exists $data->{$k} ? $data->{$k} : $v, $value_type );
                               } );
#print STDERR Data::Dumper->Dump([$slice,"SLUICER3"]);
    return $slice;
}

sub clear {
    my ($self) = @_;
    my $data = $self->data;
    if (scalar(keys %$data)) {
        $self->dirty;
    }
    %$data = ();
}

sub set {
    my ($self, $key, $value) = @_;
    my $data = $self->data;
    my $inval = $self->store->xform_in( $value, $self->{value_type} );
    unless (exists $data->{$key} && $data->{$key} eq $inval) {
        $self->dirty;
        $data->{$key} = $inval;
    }
    return $value;
}

sub delete_key {
    my ($self, $key) = @_;
    my $data = $self->data;
    return undef unless exists $data->{$key};

    if (exists $data->{$key}) {
        my $val = $data->{$key};
        $data->{$key} = undef; #set as undef so it will be delete in the db
        $self->dirty if defined $val;
        return $self->store->xform_out( $val, $self->{value_type} );
    }
}

sub tied_hash {
    my $self = shift;
    # load in hash ref from database
    my $tied_hash = $self->{tied_hash};
    return $tied_hash if $tied_hash;

    $tied_hash = $self->{tied_hash} = {};

    tie %$tied_hash, 'Yote::SQLObjectStore::TiedHash', $self;

    return $tied_hash;
}

sub load_from_sql {
    my $self = shift;

    my $data     = $self->{data};
    my $store    = $self->store;
    my $table    = $self->{table};

    $store->apply_query_array(
        "SELECT hashkey,val FROM $table WHERE id=?",
        [$self->id],
        sub {
            my ($k, $v) = @_;
            $data->{$k} = $v;
        } );
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


