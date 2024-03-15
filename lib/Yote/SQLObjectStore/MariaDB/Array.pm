package Yote::SQLObjectStore::MariaDB::Array;

use 5.16.0;

use warnings;

use base 'Yote::SQLObjectStore::BaseArray';

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

    if (@$data) {
        my $val_field = $table eq 'ARRAY_REF' ? 'refid' : 'val';
        push @queries, [ "INSERT INTO $table (id, idx, $val_field) VALUES "
                         . join( ",", ('(?,?,?)') x @$data),
                         map { ($id, $_, $data->[$_]) } (0..$#$data) ];
    }
    return $id, $table, @queries;
}


1;
