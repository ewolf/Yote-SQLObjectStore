package Yote::SQLObjectStore::SQLite::Obj;

use 5.16.0;

use warnings;

use base 'Yote::SQLObjectStore::Obj';

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
    my ($self,$force_insert) = @_;
    
    my $id = $self->id;
    my $data = $self->data;
    my $table = $self->table_name;
    my @col_names = $self->col_names;

    my ($sql);
print STDERR Data::Dumper->Dump([$data,"SAVE SQL DATA"]);
    my @qparams = map { $data->{$_} } @col_names;
    if( $id ) {
        if ($force_insert) {
            # the root object case uses this force insert
            $sql = "INSERT INTO $table (".
                join(',', 'id', @col_names).") VALUES (".
                join(',', ('?') x (1+@col_names) ).
                ") ON CONFLICT(id) DO UPDATE SET ".
                join(',', map { "$_=?" } @col_names );
            (@qparams) = ($id, @qparams, @qparams);
        } else {
            $sql = "UPDATE $table SET ".
                join(',',  map { "$_=?" } @col_names ).
                " WHERE id=?";
            push @qparams, $id;
        }
    } else {
        $sql = "INSERT INTO $table (".
            join(',', 'id', @col_names).") VALUES (".
            join(',', ('?') x (1+@col_names) ).
           ")";
    }
    return $id, $table, [$sql, @qparams];
}


1;
