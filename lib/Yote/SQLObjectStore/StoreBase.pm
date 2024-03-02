package Yote::SQLObjectStore::StoreBase;

use 5.16.0;
use warnings;

no warnings 'uninitialized';

use Yote::ObjectStore::Array;
use Yote::ObjectStore::Obj;
use Yote::ObjectStore::Hash;

warn "Yote::Locker needs to be a service somewhere";
use Yote::Locker;

use Scalar::Util qw(weaken);
use Time::HiRes qw(time);

use vars qw($VERSION);

$VERSION = '2.06';

use constant {
    DB_HANDLE    => 0,
    DIRTY        => 1,
    WEAK         => 2,
    OPTIONS      => 3,
    CACHE        => 4,
    ROOT_PACKAGE => 5,
    STATEMENTS   => 6,
};

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

    my $root_package = $args{ROOT_PACKAGE};
    
    return bless [
        $dbh,   # 0
        {},     # 1
        {},     # 2
        \%args, # 3
        {},     # 4
        $root_package,
        {}
        ], $pkg;
}

sub open {
    my $self = shift;

    $self->fetch_root;

    return $self;
}

sub statements {
    my $self = shift;
    return $self->[STATEMENTS];
}

sub dbh {
    my $self = shift;
    return $self->[DB_HANDLE];
}

=head2 fetch_root()

Returns the root node of the object store.

=cut

sub fetch_root {
    my $self = shift;
    my $root_package = $self->[ROOT_PACKAGE];

    my $root_id = 1;

    my $root = $self->fetch( $root_id );
    return $root if $root;

    $root = $self->new_obj( $self->[ROOT_PACKAGE] );

} #fetch_root


=head2 save (obj)

If given an object, saves that object.

If not given an object, saves all objects marked dirty.

=cut

sub save {
    my ($self,$obj) = @_;
    my @dirty = $obj ? ([undef,$obj]) : values %{$self->[DIRTY]};

    # start transaction
    for my $pair (@dirty) {
        $self->store_obj_data_to_sql( $pair->[1] );
    }
    %{$self->[DIRTY]} = ();

    # end transaction
    return 1;
} #save


=head2 fetch( $id )

Returns the object with the given id.

=cut

sub fetch {
    my ($self, $id) = @_;
    my $obj;
    if (exists $self->[DIRTY]{$id}) {
        $obj = $self->[DIRTY]{$id}[0];
    } else {
        $obj = $self->[WEAK]{$id};
    }
 
    return $obj if $obj;


    $obj = $self->fetch_obj_from_sql( $id );

    return undef unless $obj;

    $self->weak( $id, $obj );

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
            $fetched = ref($fetched) eq 'HASH' ? $fetched->{$list_name} : $fetched->get( $list_name );
            $fetched = $fetched->[$idx];
        } else {
            $fetched = ref($fetched) eq 'HASH' ? $fetched->{$segment} : $fetched->get($segment);
        }
        last unless defined $fetched;
    }
    return $fetched;
}

=head2 tied_item( $obj )

If the object is tied 
(like Yote::ObjectStore::Array or (like Yote::ObjectStore::Hash) it returns the unlerlying tied object.

=cut

sub tied_item {
    my ($self, $item) = @_;
    my $r = ref( $item );
    my $tied = $r eq 'ARRAY' ? tied @$item
	: $r eq 'HASH' ? tied %$item
	: $item;
    return $tied;
} #tied_item


=head2 is_dirty(obj)

Returns true if the object need saving.

=cut

sub is_dirty {
    my ($self,$obj) = @_;
    my $id = $self->id_for_item( $obj );
    return defined( $self->[DIRTY]{$id} );
}

=head2 id(obj)

Returns id of object, creating it if necessary.

=cut
sub id_for_item {
    my ($self, $item) = @_;
    my $tied = $self->tied_item($item);
    return $tied->id;
} #next_id

# make a weak reference of the reference
# and save it by id
sub weak {
    my ($self,$id,$ref) = @_;
    $self->[WEAK]{$id} = $ref;

    weaken( $self->[WEAK]{$id} );
}

#
# make sure the given obj has a weak
# reference, and is stored by the id
# in the DIRTY cache
#
sub dirty {
    my ($self,$id,$obj) = @_;
    unless ($self->[WEAK]{$id}) {
	$self->weak($id,$obj);
    }
    my $target = $self->[WEAK]{$id};

    my @dids = keys %{$self->[DIRTY]};

    my $tied = $self->tied_item( $target );

    $self->[DIRTY]{$id} = [$target,$tied];
} #dirty

"BUUG";

=head1 AUTHOR
       Eric Wolf        coyocanid@gmail.com

=head1 COPYRIGHT AND LICENSE

       Copyright (c) 2012 - 2024 Eric Wolf. All rights reserved.  This program is free software; you can redistribute it and/or modify it
       under the same terms as Perl itself.

=head1 VERSION
       Version 1.00  (Feb, 2024))

=cut

