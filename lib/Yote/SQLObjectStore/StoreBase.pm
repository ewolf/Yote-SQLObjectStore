package Yote::SQLObjectStore::StoreBase;

use 5.16.0;
use warnings;

no warnings 'uninitialized';

use Yote::SQLObjectStore::TiedArray;
use Yote::SQLObjectStore::TiedHash;

warn "Yote::Locker needs to be a service somewhere";
use Yote::Locker;

use Scalar::Util qw(weaken blessed);
use Time::HiRes qw(time);

use vars qw($VERSION);

$VERSION = '2.06';

=head1 NAME

Yote::SQLObjectStore - store and lazy load perl objects, hashes and arrays.

=head1 SYNOPSIS

 use Yote::ObjectStore;
 use Yote::RecordStoreStore;

 my $rec_store = Yote::ObjectStore::open_store( "/path/to/data" );

 my $obj_store = Yote::ObjectStore::open_object_store( $rec_store );

 $obj_store->lock;

 my $root = $obj_store->fetch_root;

 my $foo = $root->get_foo( "default-value" );

 my $list = $root->set_list( [ "A", { B => "B" }, 'c' ] );

 $obj_store->save;

 $obj_store->unlock;

=head1 DESCRIPTION

Yote::ObjectStore

=head1 METHODS

=head2 open_object_store( $directory )

If just given a string, it is assumed to be a directory and
a Yote::RecordStore is loaded from that directory.

=head2 open_object_store( %args )

Options

=over 4

=item record_store - required. a record store

=item logger - optional, a logger like Yote::ObjectStore::HistoryLogger

=item logdir - optional. if logger is not given but logdir is, attaches a Yote::ObjectStore::HistoryLogger set up to write in that directory.

=back

=cut

sub new {
    my( $pkg, %args ) = @_;

    my $dbh = $pkg->connect_sql( %args );

    my $store = bless {
        DBH          => $dbh,
        DIRTY        => {},
        WEAK         => {},
        OPTIONS      => \%args,
        ROOT_PACKAGE => $args{root},
        STATEMENTS   => {},
    }, $pkg;

    $store->make_table_manager;

    return $store;

}

sub open {
    my $self = shift;

    $self->fetch_root;

    return $self;
}

sub statements {
    my $self = shift;
    return $self->{STATEMENTS};
}

sub dbh {
    my $self = shift;
    return $self->{DBH};
}

sub get_table_manager {
    my $self = shift;
    return $self->{TABLE_MANAGER};
}

sub store_obj_data_to_sql {
    my ($self, $ref ) = @_;

    my $obj = _yoteobj($ref);
    my (@queries) = $obj->save_sql;
    for my $q (@queries) {
        my ($update_obj_table_sql, @qparams) = @$q;
        $self->query_do( $update_obj_table_sql, @qparams );
    }
}

sub record_count {
    my $self = shift;

    my ($count) = $self->query_line( "SELECT count(*) FROM ObjectIndex WHERE live=1" );

    return $count;
}

sub make_all_tables {
    my $self = shift;
    my @sql = $self->make_all_tables_sql;
    $self->start_transaction;
    for my $s (@sql) {
        my ($query, @qparams) = @$s;
        $self->query_do( $query, @qparams );
    }
    $self->commit_transaction;

}

sub make_all_tables_sql {
    my $self = shift;
    my $manager = $self->get_table_manager;
    my @sql = $manager->generate_tables_sql( $self->base_obj );
    return @sql;
}

sub check_table {
    my ($self, $table_label) = @_;
    my $manager = $self->get_table_manager;
    my $name2table = {};
    $manager->generate_reference_table( $name2table, $table_label );
    my @sql = $manager->tables_sql_updates( $name2table );

    $self->start_transaction;
    for my $s (@sql) {
        my ($query, @qparams) = @$s;
        $self->query_do( $query, @qparams );
    }
    $self->commit_transaction;
}

sub new_obj($*@) {
    my ($self, $pkg, %args) = @_;
    my $package_file = $pkg;
    $package_file =~ s/::/\//g;
    require "$package_file.pm";

    my $table = $pkg->table_name;

    my $id = $self->next_id( $table, $pkg );

    $self->check_table($pkg);

    my $obj_data = {};
    my $obj = $pkg->new(
        ID    => $id,

        data  => $obj_data,
        store => $self,
        table => $table,
        type  => "*$pkg",

        initial_vals  => \%args,
        );

    $self->weak( $obj );
    $self->dirty( $obj );

    if (%args) {
        my $cols = $pkg->cols;

        for my $input_field (keys %args) {
            if ( my $coldef = $cols->{$input_field} ) {
                $obj_data->{$input_field} = $self->xform_in( $args{$input_field}, $coldef );
            } else {
                warn "'$input_field' does not exist for object with package $pkg";
            }
        }
    }

    return $obj;
}

sub _new_thing {
    my ($self, $type, @args) = @_;
    if ($type =~ /^\*ARRAY_/) {
        return $self->new_array( $type, @args );
    }
    elsif ($type =~ /^\*HASH<\d+>_/) {
        return $self->new_hash( $type, @args );
    }
    my ($pkg) = ( $type =~ /^\*(.*)/ );
    return $self->new_obj( $pkg, @args );
}

sub new_hash {
    my ($self, $type, %args) = @_;

    my ($value_type) = ($type =~ /^\*HASH<\d+>_(.*)/);

    die "Cannot create hash of type '$type'" unless $value_type;

    my $id = $self->next_id( $type );

    $self->check_table( $type );

    my $hash = Yote::SQLObjectStore::TiedHash->tie( $self, $id, $type );
    my $tied = _yoteobj( $hash );
    $self->weak( $tied );
    $self->dirty( $tied );

    for my $key (keys %args) {
        $hash->{$key} = $args{$key};
    }

    return $hash;
}

sub has_id {
    my ($self,$ref) = @_;
    my $r = ref $ref;
    return (tied @$ref)->id if $r eq 'ARRAY';
    return (tied %$ref)->id if $r eq 'HASH';
    return blessed $ref && $ref->can('id') && $ref->id;
}

sub _yoteobj {
    my $ref = shift;
    my $r = ref $ref;
    return tied @$ref if $r eq 'ARRAY';
    return tied %$ref if $r eq 'HASH';
    return $ref;
}

sub new_array {
    my ($self, $type, @vals) = @_;

    my ($value_type) = ($type =~ /^\*ARRAY_(.*)/);

    die "Cannot create array of type '$type'" unless $value_type;

    my $id = $self->next_id( $type );
    $self->check_table( $type );

    my $array = Yote::SQLObjectStore::TiedArray->tie( $self, $id, $type );
    my $tied = _yoteobj( $array );
    $self->weak( $tied );
    $self->dirty( $tied );

    push @$array, @vals;
    return $array;
}


# given a thing and its type definition
# return its internal value which will
# either be an object id or a string value
sub xform_in {
    my $self = shift;
    my $encoded = $self->xform_in_full(@_);
    return $encoded;
}

sub xform_in_full {
    my ($self, $value, $type_def) = @_;

    my $ref = ref( $value );

    # check if type is right
    my $field_value;
    if ($type_def =~ /^\*/ && $value) {
        my $obj = _yoteobj( $value );
        unless ($self->check_type( $obj, $type_def )) {
            my $checked_type = (ref $obj && $obj->{type}) || 'scalar value';
            die "incorrect type '$checked_type' for '$type_def'";
        }
        $field_value = $obj->id;
    } else {
        $field_value = $value;
    }

    return $value, $field_value;
}

# given an internal value and definition
# return the object or string value it represents
# if it is a reference with a 0 id, return 0 indicating
# not here
sub xform_out {
    my ($self, $value, $def) = @_;

    if( $def !~ /^\*/ || $value == 0 ) {
        return $value;
    }

    # other option is a reference and the value is an id

    return $self->fetch( $value );
}


sub root_id {
    1; #always 1
}

=head2 fetch_root()

Returns the root node of the object store.

=cut

sub fetch_root {
    my $self = shift;

    my $root_id = $self->root_id;

    my $root = $self->fetch( $root_id );
    return $root if $root;

    $root = $self->new_obj( $self->{ROOT_PACKAGE} );

} #fetch_root


=head2 save (obj)

If given an object, saves that object.

If not given an object, saves all objects marked dirty.

=cut

sub save {

    my ($self,$obj) = @_;
    my @dirty = $obj ? ($obj) : values %{$self->{DIRTY}};

    # start transaction
    $self->start_transaction;
    for my $item (@dirty) {
        $self->store_obj_data_to_sql( $item );
    }
    %{$self->{DIRTY}} = ();

    $self->commit_transaction;

    # end transaction
    return 1;
} #save

=head2 fetch( $id )

Returns the object with the given id.

=cut

sub fetch {
    my ($self, $id) = @_;
    my $obj;
    if (exists $self->{DIRTY}{$id}) {
        $obj = $self->{DIRTY}{$id};
    } else {
        $obj = $self->{WEAK}{$id};
    }

    return $obj if $obj;

    $obj = $self->fetch_obj_from_sql( $id );

    return undef unless $obj;

    $self->weak( $obj );

    return $obj;
}

#
# given a from_id referencing a container, and a key or index to that 
# container, return one of the following
#   0, 'value'
#   ref_id, undef
#
sub _fetch_refid_or_val {
    my ($self, $from_id, $key_or_index) = @_;

    # check the cache first
    my $obj = $self->in_cache($from_id);
    if ($obj) {
        my $ref = _yoteobj( $obj );
        if (ref $ref eq 'Yote::SQLObjectStore::TiedArray') {
            if ($ref->{value_type} =~ /^\*/) {
                return $ref->{data}[$key_or_index], undef;
            }
            return 0, $ref->get( $key_or_index );
        }
        elsif (ref $ref eq 'Yote::SQLObjectStore::TiedHash') {
            if ($ref->{value_type} =~ /^\*/) {
                return $ref->{data}{$key_or_index}, undef;
            }
            return 0, $ref->get( $key_or_index );
        }
        my $field = $ref->cols->{$key_or_index};
        if ($field =~ /^\*/) {
            return $ref->{data}{$key_or_index}, undef;
        }
        return 0, $ref->get( $key_or_index );
    }

    # its not in the cache, so load from store
    my $table_manager = $self->get_table_manager;
    my ($tablehandle,$objectclass) = $self->query_line( "SELECT tablehandle,objectclass FROM ObjectIndex WHERE id=?", $from_id );
    my $table = $table_manager->label_to_table( $tablehandle );
    if ($tablehandle =~ /^\*ARRAY_(\*)?/) {
        my ($val) = $self->query_line( "SELECT val FROM $table WHERE id=? AND idx=?", $from_id, $key_or_index );
        if ($1) {
            # a reference
            return $val, undef;
        }
        return 0, $val;
    }
    elsif ($tablehandle =~ /^\*HASH\<\d+\>_(\*)?/) {
        my ($val) = $self->query_line( "SELECT val FROM $table WHERE id=? AND hashkey=?", $from_id, $key_or_index );
        if ($1) {
            # a reference
            return $val, undef;
        }
        return 0, $val;
    }

    # load the object class
    my $package_file = $objectclass;
    $package_file =~ s/::/\//g;

    require "$package_file.pm";

    die "Invalid Column Name for yote '$key_or_index'" if $key_or_index =~ /[^-_a-zA-Z0-9]/;
    my ($val) = $self->query_line( "SELECT $key_or_index FROM $table WHERE id=?", $from_id );

    my $cols = $objectclass->cols;
    if ($cols->{$key_or_index} =~ /^\*/) {
        # a reference
        return $val, undef;
    }
    return 0, $val;
}

# get a path from the data structure
sub fetch_path {
    my ($self, $path) = @_;
    my @path = grep { $_ ne '' } split '/', $path;

    my $from_id = $self->root_id;
    while (defined(my $segment = shift @path)) {
        my ($ref_id,$val) = $self->_fetch_refid_or_val( $from_id, $segment );
        if ($ref_id) {
            $from_id = $ref_id;
        } else {
            die "invalid path '$path', '$segment' is not a reference" if @path;
            return $val;
        }
    }
    return $self->fetch( $from_id );
}

sub ensure_path {
    my ($self, $path) = @_;
    my @path = grep { $_ ne '' } split '/', $path;

    my $current_ref = $self->fetch_root; # always a root
    my $from_id = $self->root_id;

    $self->start_transaction();
print STDERR Data::Dumper->Dump([\@path,$@,"PATH ENS"]);
    my $new_value;
    while (my $segment = shift @path) {
        my ($key_or_index, $insert_type_or_value) = ( $segment =~ /^([^{]+)(\{[^\}]+\})?$/ );
        if (!$key_or_index) {
            $self->rollback_transaction();
            die "invalid path '$path', '$segment' is malformed";
        }
        if ($insert_type_or_value) {
            ( $insert_type_or_value ) = ( $insert_type_or_value =~ /^\{(.+)\}$/ );
        }

        #
        # see if the key has a value already. if there is a ref_id, then val
        # is a reference. If there isno ref_id, val is a scalar value.
        #
        my ($ref_id,$val) = $self->_fetch_refid_or_val( $from_id, $key_or_index );

print STDERR Data::Dumper->Dump([\@path,"REMPATH"]);
print STDERR "'$segment'($key_or_index) -> $ref_id,$val,$insert_type_or_value\n";

        #
        # key is there, then check if its an array element that is there.
        # if there after the check, set the current from_id and loop again
        #
        if ($ref_id) {
            $from_id = $ref_id;
            $current_ref = $self->fetch( $ref_id );
            next;
        } elsif (defined $val) {
            if (@path) {
                $self->rollback_transaction();
                die "invalid path '$path', '$segment' is not a reference";
            }
            if ($insert_type_or_value && $insert_type_or_value ne $val) {
                $self->rollback_transaction();
                die "value at '$path' is '$val', not '$insert_type_or_value'";
            }
            $self->commit_transaction();
            return $val;
        }

        #
        # nothing is yet keyed to that segment, so create the thing and fill it
        #
        # the thing is attached to the current ref. is the current ref
        # an array, hash or class?
        #
        my $curr_obj = _yoteobj( $current_ref );

        my $is_hash  = $curr_obj->isa('Yote::SQLObjectStore::TiedHash');
        my $is_array = (! $is_hash) && $curr_obj->isa('Yote::SQLObjectStore::TiedArray');

        if ($is_array && $key_or_index !~ /^[0-9]+$/) {
            $self->rollback_transaction();
            die "invalid path '$path', array access expects index";
        }
        if ($is_hash && length( $key_or_index ) > $curr_obj->key_size) {
            $self->rollback_transaction();
            die "invalid path '$path', '$segment' key too large";
        }

        #
        # what type does the object want for the field?
        #
        my $value_type = $curr_obj->value_type( $key_or_index );
print STDERR Data::Dumper->Dump([$value_type,"VEET"]);
        if ($value_type =~ /^\*/) { # reference
            if ($insert_type_or_value && ! $curr_obj->is_type( $insert_type_or_value )) {
                $self->rollback_transaction();
print STDERR Data::Dumper->Dump([$curr_obj,"CURRO"]); # 
                die "invalid path '$path', '$segment' is wrong type. Expected '$value_type'";
            }

            eval {
                $new_value = $self->_new_thing( $value_type );
                $from_id = _yoteobj( $new_value )->id;
            };
            if ($@ || ! $new_value) {
print STDERR Data::Dumper->Dump([$@,$new_value,$value_type,"HUH"]);
                $self->rollback_transaction();
                return;
            }

        } elsif( @path ) {
            $self->rollback_transaction();
            die "invalid path '$path', '$segment' is not defined to be a reference";
        } else {
            $new_value = $insert_type_or_value;
        }

        #
        # install the new thing at the path reference
        #
        if ($is_array) {
            $current_ref->[$key_or_index] = $new_value;
        }
        elsif ($is_hash) {
            $current_ref->{$key_or_index} = $new_value;
        }
        else {
            $current_ref->set( $key_or_index, $new_value );
        }
        $current_ref = $new_value;
    }

    $self->commit_transaction();
    return $new_value;
} #ensure_path


=head2 is_dirty(obj)

Returns true if the object is a base storable object that needs saving.

=cut

sub is_dirty {
    my ($self,$ref) = @_;
    my $obj = _yoteobj($ref);
    unless ( blessed $obj and $obj->isa('Yote::SQLObjectStore::BaseStorable')
             || $obj->isa('Yote::SQLObjectStore::TiedArray')
             || $obj->isa('Yote::SQLObjectStore::TiedHash') )
    {
        warn "checked if non base value is dirty";
        return;
    }

    return defined( $self->{DIRTY}{$obj->id} );
}

sub in_cache {
    my ($self, $id) = @_;
    return $self->{DIRTY}{$id} || $self->{WEAK}{$id};
}

# make a weak reference of the reference
# and save it by id
sub weak {
    my ($self,$ref) = @_;
    my $obj = _yoteobj($ref);
    my $id = $obj->id;
    my $cache_obj = $obj->cache_obj;
    $self->{WEAK}{$id} = $cache_obj;

    weaken( $self->{WEAK}{$id} );
}

#
# make sure the given obj has a weak
# reference, and is stored by the id
# in the DIRTY cache
#
sub dirty {
    my ($self,$ref) = @_;
    my $obj = _yoteobj($ref);
    return unless blessed $obj;
    my $id = $obj->id;
    unless ($self->{WEAK}{$id}) {
	$self->weak($obj);
    }
    my $target = $self->{WEAK}{$id};

    my @dids = keys %{$self->{DIRTY}};

    $self->{DIRTY}{$id} = $target;
} #dirty

sub next_id {
    my ($self, $table_handle, $objectclass) = @_;
    return $self->insert_get_id( "INSERT INTO ObjectIndex (tablehandle,objectclass,live) VALUES (?,?,1)", $table_handle, $objectclass );
}


# --------- DB FUNS -------

sub sth {
    my ($self, $query ) = @_;

    my $stats = $self->statements;
    my $dbh   = $self->dbh;
    my $sth   = ($stats->{$query} //= $dbh->prepare( $query ));

    $sth or die "$query : ". $dbh->errstr;

    return $sth;
}

sub insert_get_id {
    my ($self, $query, @qparams ) = @_;
    my $dbh = $self->dbh;
    my $sth = $self->sth( $query );
    my $res = $sth->execute( @qparams );
    if (!defined $res) {
        die $sth->errstr;
    }
    my $id = $dbh->last_insert_id;
    return $id;
}

sub query_do {
    my ($self, $query, @qparams ) = @_;
    my $dbh = $self->dbh;
    my $sth = $dbh->prepare( $query );
    if (!defined $sth) {
        die $dbh->errstr;
    }
    my $res = $sth->execute( @qparams );
    if (!defined $res) {
use Carp 'longmess'; print STDERR Data::Dumper->Dump([longmess,$query,\@qparams,$sth->errstr,"BADO"]);
        die $sth->errstr;
    }
    my $id = $dbh->last_insert_id;
    return $id;
}

sub query_line {
    my ($self, $query, @qparams ) = @_;
    my $sth = $self->sth( $query );

    my $res = $sth->execute( @qparams );
    if (!defined $res) {
        die $sth->errstr;
    }
    my @ret = $sth->fetchrow_array;
    return @ret;
}

sub print_query_output {
    my ($self, $query, @qparams ) = @_;
    my $sth = $self->sth( $query );
    my $res = $sth->execute( @qparams );
    if (!defined $res) {
        die $sth->errstr;
    }
}

sub apply_query_array {
    my ($self, $query, $qparams, $eachrow_fun ) = @_;
    my $sth = $self->sth( $query );
    my $res = $sth->execute( @$qparams );
    if (!defined $res) {
        die $sth->errstr;
    }
    while ( my (@array) = $sth->fetchrow_array ) {
        $eachrow_fun->(@array);
    }
}

sub start_transaction {
    my $self = shift;
    return if $self->{in_transaction};
    $self->_start_transaction;
    $self->{in_transaction} = 1;
}
sub commit_transaction {
    my $self = shift;
    return unless $self->{in_transaction};
    $self->_commit_transaction;
    $self->{in_transaction} = 0;
}
sub rollback_transaction {
    my $self = shift;
    return unless $self->{in_transaction};
    $self->_rollback_transaction;
    $self->{in_transaction} = 0;
}

sub fetch_obj_from_sql {
    my ($self, $id) = @_;

    my ($handle,$class) = $self->query_line(
        "SELECT tablehandle, objectclass FROM ObjectIndex WHERE id=?",
        $id );

    return undef unless $handle;

    if ($handle =~ /^\*HASH<\d+>_.*/) {
        return Yote::SQLObjectStore::TiedHash->tie( $self, $id, $handle );
    }
    elsif ($handle =~ /^\*ARRAY_.*/) {
        return Yote::SQLObjectStore::TiedArray->tie( $self, $id, $handle );
    }

    # otherwise is an object, so grab its data

    my $table = $handle;

    my $package_file = $class;
    $package_file =~ s/::/\//g;

    require "$package_file.pm";

    my $cols = $class->cols;
    my @cols = keys %$cols;

    my $sql = "SELECT ".join(',', @cols )." FROM $table WHERE id=?";

    my (@ret) = $self->query_line( $sql, $id );

    my $obj = $class->new(
        ID => $id,

        data => { map { $cols[$_] => $ret[$_] } (0..$#cols) },
        has_first_save => 1,
        store => $self,
        table => $table,
        type  => "*$class",

        );

    return $obj;
}

sub check_type {
    my ($self, $value, $type_def) = @_;
    my $obj = _yoteobj($value);

    $obj
        and
        $obj->isa( $self->base_obj ) ||
        $obj->isa( 'Yote::SQLObjectStore::TiedArray' ) ||
        $obj->isa( 'Yote::SQLObjectStore::TiedHash' )
        and
        $obj->is_type( $type_def );
}

"BUUG";
