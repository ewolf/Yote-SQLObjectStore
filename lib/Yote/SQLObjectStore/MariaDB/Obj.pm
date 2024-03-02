package Yote::SQLObjectStore::SQLite::Obj;

use 5.16.0;

use warnings;

use base 'Yote::SQLObjectStore::BaseObj';

use Yote::SQLObjectStore::SQLite::Hash;
use Yote::SQLObjectStore::SQLite::Array;

sub make_table_sql {
    my $pkg = shift;
    my $cols = $pkg->cols;
    my %converted_cols;
    for my $key (keys %$cols) {
        my $val = $cols->{$key};
        if ($val =~ /(ARRAY|HASH)_(REF|VALUE)/) {
             $val = "INT";
        }
        $converted_cols{$key} = $val;
    }

    my $table = $pkg->table_name;
    my $sql = "CREATE TABLE IF NOT EXISTS $table (".
        join( ",", "id INT PRIMARY KEY", 
              map { "$_ $converted_cols{$_}" } sort keys %converted_cols ) .
        ")";
}

sub save_sql {
    my ($self) = @_;
    
    my $id = $self->id;
    my $data = $self->data;
    my $table = $self->table_name;
    my @col_names = $self->col_names;

    my ($sql);

    my @qparams = map { $data->{$_} } @col_names;
    if( $self->_has_first_save ) {
        $sql = "UPDATE $table SET ".
            join(',',  map { "$_=?" } @col_names ).
            " WHERE id=?";
        push @qparams, $id;
    } 
    else {
        $sql = "INSERT INTO $table (".
            join(',', 'id', @col_names).") VALUES (".
            join(',', ('?') x (1+@col_names) ).
           ")";
        unshift @qparams, $id;
    }
    return $id, $table, [$sql, @qparams];
}


1;
