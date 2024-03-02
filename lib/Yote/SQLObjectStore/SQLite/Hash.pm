package Yote::SQLObjectStore::SQLite::Hash;

use 5.16.0;

use warnings;

use base 'Yote::SQLObjectStore::BaseHash';

sub save_sql {
    my ($self) = @_;
    
    my $id = $self->id;
    my $table = $self->table_name;
    
    my @queries;

    # delete old rows and insert new ones
    my $del_old_sql = "DELETE FROM $table WHERE id=?";
    push @queries, [ $del_old_sql, $id ];

    # insert new
    my $data = $self->data;

    my $val_field = $table eq 'HASH_REF' ? 'refid' : 'val';

    my @fields = keys %$data;

    if (@fields) {
        push @queries, [ "INSERT INTO $table (id, key, $val_field) VALUES "
                         . join( ",", ('(?,?,?)') x @fields ),
                         map { ($id, $_, $data->{$_}) } @fields ];
    }
    
    return $id, $table, @queries;
}

1;
