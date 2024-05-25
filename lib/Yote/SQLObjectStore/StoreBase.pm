package Yote::SQLObjectStore::StoreBase;

use 5.16.0;
use warnings;

no warnings 'uninitialized';

use Yote::SQLObjectStore::Array;
use Yote::SQLObjectStore::Hash;

warn "Yote::Locker needs to be a service somewhere";
use Yote::Locker;

use Scalar::Util qw(weaken);
use Time::HiRes qw(time);

use vars qw($VERSION);

$VERSION = '2.06';

=head1 NAME

Yote::ObjectStore - store and lazy load perl objects, hashes and arrays.

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
        ROOT_PACKAGE => $args{ROOT_PACKAGE},
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
    my ($self, $obj ) = @_;

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
    $self->query_do( "BEGIN" );
    for my $s (@sql) {
        my ($query, @qparams) = @$s;
        print STDERR "MAKING > $query\n";
        $self->query_do( $query, @qparams );
    }
    $self->query_do( "COMMIT" );
}

sub make_all_tables_sql {
    my $self = shift;
    my $manager = $self->get_table_manager;
    my @sql = $manager->generate_tables_sql( $self->base_obj );
    return @sql;
}



sub new_obj($*@) {
    my ($self, $pkg, %args) = @_;
    my $package_file = $pkg;
    $package_file =~ s/::/\//g;
    require "$package_file.pm";

    my $table = join '_', reverse split /::/, $pkg;

    my $id = $self->next_id( $table );

    my $obj_data = {};
    my $obj = $pkg->new( 
        ID    => $id,

        data  => $obj_data,
        store => $self,
        table => $self->get_table_manager->label_to_table($pkg),
        type  => "*$pkg",
        
        %args );

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

sub new_hash {
    my ($self, $type, %args) = @_;

    my ($key_size, $value_type) = ($type =~ /^\*HASH<(\d+)>_(.*)/);

    die "Cannot create hash of type '$type'" unless $value_type;

    my $id = $self->next_id( $type );

    my $data = {};

    my $hash = Yote::SQLObjectStore::Hash->new(
        ID         => $id,
        
        data       => $data,
        key_size   => $key_size,
        store      => $self,
        table      => $self->get_table_manager->label_to_table($type),
        type       => $type,
        value_type => $value_type,
        );

    $self->weak( $hash );
    $self->dirty( $hash );

    for my $input_field (keys %args) {
        $data->{$input_field} = $self->xform_in( $args{$input_field}, $value_type );
    }
    return $hash;
}


sub new_array {
    my ($self, $type, @vals) = @_;

    my ($value_type) = ($type =~ /^\*ARRAY_(.*)/);

    die "Cannot create array of type '$type'" unless $value_type;


    my $id = $self->next_id( $type );

    my $data = [];

    my $table = $self->get_table_manager->label_to_table($type);

    
    
    my $array = Yote::SQLObjectStore::Array->new(
        ID    => $id,
        
        data  => $data,
        store => $self,
        table => $table,
        type  => $type,
        value_type => $value_type,

        );
    
    $self->weak( $array );
    $self->dirty( $array );

    push @$data, map { $self->xform_in( $_, $value_type ) } @vals;

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

    die "xform_in_full called without data definition" unless $type_def;

    my $ref = ref( $value );

    # check if type is right
    my $field_value;
    if ($type_def =~ /^\*/ && $value) {
        die "incorrect type '$type_def' for '$value'" unless $self->check_type( $value, $type_def );
        $field_value = $value->id;
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

    die "xform_out called without data definition" unless $def;

    if( $def !~ /^\*/ || $value == 0 ) {
        return $value;
    }

    # other option is a reference and the value is an id

    return $self->fetch( $value );
}


=head2 fetch_root()

Returns the root node of the object store.

=cut

sub fetch_root {
    my $self = shift;

    # the root always has id 1
    my $root_id = 1;

    my $root = $self->fetch( $root_id );
    return $root if $root;

    $root = $self->new_obj( $self->{ROOT_PACKAGE}, 
                            store => $self );

} #fetch_root


=head2 save (obj)

If given an object, saves that object.

If not given an object, saves all objects marked dirty.

=cut

sub save {

    my ($self,$obj) = @_;
    my @dirty = $obj ? ($obj) : values %{$self->{DIRTY}};

    # start transaction
    for my $item (@dirty) {
        $self->store_obj_data_to_sql( $item );
    }
    %{$self->{DIRTY}} = ();

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

# get a path from the data structure
sub fetch_path {
    my ($self, $path) = @_;
    my @path = grep { $_ ne '' } split '/', $path;
    my $fetched = $self->fetch_root;

    while (my $segment = shift @path) {
        if ($segment =~ /(.*)\[(\d+)\]$/) { #list or list segment
            my ($list_name, $idx) = ($1, $2);
            $fetched = $fetched->get( $list_name );
            $fetched = $fetched->get( $idx );
        } else {
            $fetched = $fetched->get($segment);
        }
        last unless defined $fetched;
    }
    return $fetched;
}

sub ensure_path {
    my ($self, $path) = @_;
    my @path = grep { $_ ne '' } split '/', $path;
    my $fetched = $self->fetch_root;

    while (my $segment = shift @path) {
        if ($segment =~ /(.*)\[(\d*)\]$/) { #list or list segment
            my ($list_name, $idx) = ($1, $2);
            $fetched = $fetched->get( $list_name );
            return undef unless ref $fetched ne 'ARRAY';
            if ($idx ne '') {
                $fetched = $fetched->[$idx];
            } elsif( @path ) {
                # no id and more in path? nope!
                return undef;
            } else {
                return $fetched;
            }
        } else {
            $fetched = ref($fetched) eq 'HASH' ? $fetched->{$segment} : $fetched->get($segment);
        }
        last unless defined $fetched;
    }
}


=head2 is_dirty(obj)

Returns true if the object need saving.

=cut

sub is_dirty {
    my ($self,$obj) = @_;

    return defined( $self->{DIRTY}{$obj->id} );
}

# make a weak reference of the reference
# and save it by id
sub weak {
    my ($self,$ref) = @_;
    my $id = $ref->id;
    $self->{WEAK}{$id} = $ref;

    weaken( $self->{WEAK}{$id} );
}

#
# make sure the given obj has a weak
# reference, and is stored by the id
# in the DIRTY cache
#
sub dirty {
    my ($self,$obj) = @_;
    return unless $obj;
    my $id = $obj->id;
    unless ($self->{WEAK}{$id}) {
	$self->weak($obj);
    }
    my $target = $self->{WEAK}{$id};

    my @dids = keys %{$self->{DIRTY}};

    $self->{DIRTY}{$id} = $target;
} #dirty

sub next_id {
    my ($self, $table_name) = @_;

    return $self->insert_get_id( "INSERT INTO ObjectIndex (tablehandle,live) VALUES (?,1)", $table_name );
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
 print STDERR Data::Dumper->Dump([$query,\@qparams,"INSERT GET ID ($id)"]);
    return $id;
}

sub query_all {
    my ($self, $query, @qparams ) = @_;
    print STDERR Data::Dumper->Dump([$query,\@qparams,"query_all"]);
    my $dbh = $self->dbh;
    my $sth = $dbh->prepare( $query );
    if (!defined $sth) {
        die $dbh->errstr;
    }
    my $res = $sth->execute( @qparams );
    if (!defined $res) {
        die $sth->errstr;
    }
    return $sth->fetchall_hashref('id');
}


sub query_do {
    my ($self, $query, @qparams ) = @_;
    if ($query =~ /CREATE.*SomeThing_SQLite/) {
use Carp 'longmess'; print STDERR Data::Dumper->Dump([longmess]);
    }
    print STDERR Data::Dumper->Dump([$query,\@qparams,"query do"]);
    my $dbh = $self->dbh;
    my $sth = $dbh->prepare( $query );
    if (!defined $sth) {
use Carp 'longmess'; print STDERR Data::Dumper->Dump([$query,\@qparams,longmess]);
        die $dbh->errstr;
    }
    my $res = $sth->execute( @qparams );
    if (!defined $res) {
use Carp 'longmess'; print STDERR Data::Dumper->Dump([$query,\@qparams,$sth->errstr,"UNDH"]);
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

    print STDERR Data::Dumper->Dump([$query,\@qparams,\@ret,"query line"]);

    return @ret;
}

sub apply_query_array {
    my ($self, $query, $qparams, $eachrow_fun ) = @_;
    my $sth = $self->sth( $query );
    my $res = $sth->execute( @$qparams );
    if (!defined $res) {
        die $sth->errstr;
    }
print STDERR Data::Dumper->Dump([$query,$qparams,"apply query"]);
    while ( my (@arry) = $sth->fetchrow_array ) {
print STDERR Data::Dumper->Dump([\@arry,"apply query result"]);
        $eachrow_fun->(@arry);
    }
}


sub fetch_obj_from_sql {
    my ($self, $id) = @_;

    my ($handle) = $self->query_line(
        "SELECT tablehandle FROM ObjectIndex WHERE id=?",
        $id );

    return undef unless $handle;

    if ($handle =~ /^\*HASH<(\d+)>_(.*)/) {
        my $hash = Yote::SQLObjectStore::Hash->new(
            ID             => $id,

            data           => {},
            type           => $handle,
            has_first_save => 1,
            key_size       => $1,
            store          => $self,
            table          => $self->get_table_manager->label_to_table($handle),
            value_type     => $2,
            );
        $hash->load_from_sql;
        return $hash;
    }
    elsif ($handle =~ /^\*ARRAY_(.*)/) {
        return Yote::SQLObjectStore::Array->new(
            ID             => $id,

            data           => [],
            has_first_save => 1,
            store          => $self,
            table          => $self->get_table_manager->label_to_table($handle),
            value_type     => $1,
            );
    }

    # otherwise is an object, so grab its data

    my $table = $handle;
    my $class = join "::", reverse split /_/, $handle;

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
    
    $value
        and
        $value->isa( $self->base_obj ) ||
        $value->isa( 'Yote::SQLObjectStore::Array' ) ||
        $value->isa( 'Yote::SQLObjectStore::Hash' ) 
        and
        $value->is_type( $type_def );
}

"BUUG";

