package Yote::SQLObjectStore::Base;

use v5.16;
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
    DB_HANDLE => 0,
    DIRTY     => 1,
    WEAK      => 2,
    OPTIONS   => 3,
    CACHE     => 4,
    LOCKER    => 5,
    ROOT_PACKAGE => 6,

    DATA => 1,
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

sub open_object_store {
    my( $pkg, %args ) = @_;

    my $dbh = $args{DBH} // $pkg->connect_sql( %args );

    my $base_dir = $args{BASE_DIRECTORY} or die __PACKAGE__."::open_object_store called without a BASE_DIRECTORY";

    my $root_package = $args{ROOT_PACKAGE} or die __PACKAGE__."::open_object_store called without a ROOT_PACKAGE";
    
    my $locker = Yote::Locker->new( "$base_dir/LOCKER" );
    
    my $store = bless [
        $dbh,   # 0
        {},     # 1
        {},     # 2
        \%args, # 3
        {},     # 4
        $locker,
        $root_package,
        ], $pkg;

    $locker->lock;
    $store->fetch_root;
    $locker->unlock;
    return $store;
}

=head2 lock()

Locks the record store

=cut
sub lock {
    shift->[LOCKER]->lock;
}

=head2 unlock()

Unlocks the record store

=cut
sub unlock {
    shift->[LOCKER]->unlock;    
}

=head2 fetch_root()

Returns the root node of the object store.

=cut

sub fetch_root {
    my $self = shift;
    my $root_package = $self->[ROOT_PACKAGE];

#    $record_store->lock;
    my $root_id = 1;

    my $root = $self->_fetch( $root_id );

    if ($root) {
        return $root;
    }

    # directly bless this rather than call new_obj so the init can be controlled here
    $root = bless [
        $root_id,
        {},
        $self,
        {},
        ], 'Yote::ObjectStore::Obj';
    
    # special save here, just stow it 
    $record_store->stow( $root->__freezedry, $root_id );
    $self->_log_history( "SAVE", $root_id, $root->__logline );

    $self->_weak( $root_id, $root );
    
    return $root;
} #fetch_root


=head2 save (obj)

If given an object, saves that object.

If not given an object, saves all objects marked dirty.

=cut

sub save {
    my ($self,$obj) = @_;

    my $record_store = $self->[RECORD_STORE];

#    $record_store->lock;

    $record_store->use_transaction unless $self->[OPTIONS]{no_transactions};

    my $dirty = $self->[DIRTY];

    if ($obj) {
        # kind of dangerous to overall integrity
        # use with caution
        my $r = ref( $obj );
        if ($r eq 'ARRAY') {
            $obj = tied @$obj;
        }
        elsif ($r eq 'HASH') {
            $obj = tied %$obj;
        }
        my $id = $obj->id;

        my $froze = $obj->__freezedry;

        $self->_log_history( "SAVE", $id, $obj->__logline );
        $record_store->stow( $froze, $id );
        delete $dirty->{$id};
    } else {
        my @ids_to_save = keys %$dirty;

        for my $id (@ids_to_save) {
            my $obj = delete $dirty->{$id};
            $obj = $obj->[0];
            my $r = ref( $obj );
            if ($r eq 'ARRAY') {
                $obj = tied @$obj;
            }
            elsif ($r eq 'HASH') {
                $obj = tied %$obj;
            }
            my $froze = $obj->__freezedry;
            $self->_log_history( "SAVE", $id, $obj->__logline );
            $record_store->stow( $froze, $id );
        }
        %$dirty = ();
    }
    $record_store->commit_transaction unless $self->[OPTIONS]{no_transactions};

    return 1;
} #save


=head2 fetch( $id )

Returns the object with the given id.

=cut

sub fetch {
    my ($self, $id) = @_;
    return $self->_fetch( $id );
}

sub fetch_path {
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
    return $fetched;
}

# fetch_path, but with autoviv.
# returns undef if it cannot
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

sub _fetch {
    my ($self, $id) = @_;

    
    

    my $obj;
    if (exists $self->[DIRTY]{$id}) {
        $obj = $self->[DIRTY]{$id}[0];
    } else {
        $obj = $self->[WEAK]{$id};
    }
 
    return $obj if $obj;
    
    my $record_store = $self->[RECORD_STORE];

    my ( $update_time, $creation_time, $record ) = $record_store->fetch( $id );

    unless (defined $record) {
        return undef;
    }
    $obj = $self->_reconstitute( $id, $record, $update_time, $creation_time );
    $self->_weak( $id, $obj );
    return $obj;
} #fetch


=head2 tied_obj( $obj )

If the object is tied 
(like Yote::ObjectStore::Array or (like Yote::ObjectStore::Hash) it returns the unlerlying tied object.

=cut

sub tied_obj {
    my ($self, $item) = @_;
    my $r = ref( $item );
    my $tied = $r eq 'ARRAY' ? tied @$item
	: $r eq 'HASH' ? tied %$item
	: $item;
    return $tied;
} #tied_obj


=head2 existing_id( $item )

Returns the id of the given item, if it has been
assigend one yet. This is a way to check if 
an array or hash is in the store.

=cut

sub existing_id {
    my ($self, $item) = @_;
    my $r = ref( $item );
    if ($r eq 'ARRAY') {
        my $tied = tied @$item;
        if ($tied) {
            return $tied->id;
        }
	return undef;
    }
    elsif ($r eq 'HASH') {
        my $tied = tied %$item;
        if ($tied) {
            return $tied->id;
        }
	return undef;
    }
    elsif ($r && $item->isa( 'Yote::ObjectStore::Obj' )) {
	return $item->id;
    }
    return undef;

} #existing_id


=head1 cache(@objs)

Caches the objects.

=cut

sub cache {
    my ($self, @objs) = @_;
    my $cache = $self->[CACHE];
    for my $obj (@objs) {
        $cache->{$obj} = $obj; # stringified obj is id
    }
}

=head1 cache(@objs)

Uncaches the objects given.
If none given, caches all.

=cut

sub uncache {
    my ($self, @objs) = @_;
    if (@objs) {
        my $cache = $self->[CACHE];
        for my $obj (@objs) {
            delete $cache->{$obj}; # stringified obj is id
        }
    } else {
        $self->[CACHE] = {};
    }
}



=head2 is_dirty(obj)

Returns true if the object need saving.

=cut

sub is_dirty {
    my ($self,$obj) = @_;
    my $id = $self->id( $obj );
    return defined( $self->[DIRTY]{$id} );
}


# Returns true if the object has a weak reference
# in the database.
sub _id_is_referenced {
    my ($self,$id) = @_;
    return defined( $self->[WEAK]{$id} );
}



=head2 id(obj)

Returns id of object, creating it if necessary.

=cut
sub id {
    my ($self, $item) = @_;
    my $r = ref( $item );
    if ($r eq 'ARRAY') {
        my $tied = tied @$item;
        if ($tied) {
            return $tied->id;
        }
        my $id = $self->_new_id;
	my @contents = @$item;
        @$item = ();  # this is where the leak was coming from
        tie @$item, 'Yote::ObjectStore::Array', $id, $self;
	push @$item, @contents;
        $self->_weak( $id, $item );
        $self->_dirty( $id );
        return $id;
    }
    elsif ($r eq 'HASH') {
        my $tied = tied %$item;
        if ($tied) {
            return $tied->id;
        }
        my $id = $self->_new_id;
	my %contents = %$item;
        %$item = ();  # this is where the leak was coming frmo
        tie %$item, 'Yote::ObjectStore::Hash', $id, $self;
        $self->_weak( $id, $item );
	for my $key (keys %contents) {
	    $item->{$key} = $contents{$key};
	}
        $self->_dirty( $id );
        return $id;
    }
    elsif ($r && $item->isa( 'Yote::ObjectStore::Obj' )) {
	return $item->id;
    }
    return undef;

} #_id

# make a weak reference of the reference
# and save it by id
sub _weak {
    my ($self,$id,$ref) = @_;
    $self->[WEAK]{$id} = $ref;

    weaken( $self->[WEAK]{$id} );
}

#
# make sure the given obj has a weak
# reference, and is stored by the id
# in the DIRTY cache
#
sub _dirty {
    my ($self,$id,$obj) = @_;
    unless ($self->[WEAK]{$id}) {
	$self->_weak($id,$obj);
    }
    my $target = $self->[WEAK]{$id};

    my @dids = keys %{$self->[DIRTY]};

    my $tied = $self->tied_obj( $target );

    $self->[DIRTY]{$id} = [$target,$tied];
} #_dirty

#
# Convert the argument into an internal representation.
# if it is a reference, 'r' + id
# if it is undef, 'u'
# otherwise, 'v' + value
#
sub _xform_in {
    my ($self,$item) = @_;
    my $r = ref( $item );
    if ($r) {
        return 'r' . $self->id( $item );
    }
    elsif (defined $item) {
        return "v$item";
    }
    else {
        return 'u';
    }
} #_xform_in

# used by Obj,Array,Hash
# given an internal designation, return what it is
# if 'r' + id, return object of that id
# if 'v' + value, return the value string
# if 'u', return undef
sub _xform_out {
    my ($self,$str) = @_;
    return undef unless $str;
    my $tag = substr( $str, 0, 1 );
    my $val = substr( $str, 1 );
    if ($tag eq 'r') {
        #reference
        my $item = $self->fetch( $val );
        return $item;
    }
    elsif ($tag eq 'v') {
        return $val;
    }
    return undef;
} #_xform_out

sub _reconstitute {
    my ($self, $id, $data, $update_time, $creation_time ) = @_;

    my $class_length = unpack "I", $data;
    (undef, my $class) = unpack "I(a$class_length)", $data;

    my $clname = $class;
    $clname =~ s/::/\//g;

    require "$clname.pm";

    return $class->__reconstitute( $id, $data, $self, $update_time, $creation_time );

} #_reconstitute

sub _new_id {
    my $self = shift;
    my $record_store = $self->[RECORD_STORE];
    my $id = $self->[RECORD_STORE]->next_id;
    return $id;
} #_new_id


sub _log_history {
    my $logger = shift->[LOGGER];
    $logger && $logger->history( @_ );
}

=item max_id

Returs the highest id in the store.

=cut
sub max_id {
    my $rs = shift->[RECORD_STORE];
    my $c = $rs->record_count;
    return $c;
}

=item new_obj( data, class )

Create a new RecordStore object popualted with
the optional given data hash ref and optional 
child class of Yote::RecordStore::Obj. 
The arguments may be given in either order.

=cut
sub new_obj {
    my ($self, $data, $class) = @_;
    unless (ref $data) {
	($class,$data) = ($data,$class);
    }
    $data  //= {};
    $class //= 'Yote::ObjectStore::Obj';

    if( $class ne 'Yote::ObjectStore::Obj' ) {
      my $clname = $class;
      $clname =~ s/::/\//g;

      require "$clname.pm";
    }
    
    my $id = $self->_new_id;

    my $obj = bless [
        $id,
        { map { $_ => $self->_xform_in($data->{$_}) } keys %$data},
        $self,
	{},
        ], $class;
    $self->_dirty( $id, $obj );
    $obj->_init;
    $self->_weak( $id, $obj );

    return $obj;
} #new_obj

"BUUG";

=head1 AUTHOR
       Eric Wolf        coyocanid@gmail.com

=head1 COPYRIGHT AND LICENSE

       Copyright (c) 2012 - 2020 Eric Wolf. All rights reserved.  This program is free software; you can redistribute it and/or modify it
       under the same terms as Perl itself.

=head1 VERSION
       Version 2.13  (Feb, 2020))

=cut

