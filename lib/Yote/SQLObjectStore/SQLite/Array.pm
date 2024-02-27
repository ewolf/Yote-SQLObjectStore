package Yote::SQLObjectStore::SQLite::Array;

use 5.16.0;

use warnings;

use base 'Yote::SQLObjectStore::Array';

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

    my $val_field = $table eq 'ARRAY_REF' ? 'refid' : 'val';

    my @fields = keys %$data;
    push @queries, [ "INSERT INTO $table (id, idx, $val_field) VALUES "
                     . join( ",", '(?,?,?)' x @fields ),
                     map { ($id, $_, $data->[$_]) } (0..$#$data) ];
    
    return $id, $table, @queries;
}


1;