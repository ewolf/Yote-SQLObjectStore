package Yote::SQLObjectStore::BaseStorable;

use 5.16.0;

no warnings 'uninitialized';

#
# The string version of the objectstore object is simply its id. 
# This allows object ids to easily be stored as hash keys.
#
# use overload
#     '""' => sub { my $self = shift; $self->{ID} },
#     eq   => sub { ref($_[1]) && $_[1]->{ID} == $_[0]->{ID} },
#     ne   => sub { ! ref($_[1]) || $_[1]->{ID} != $_[0]->{ID} },
#     '=='   => sub { ref($_[1]) && $_[1]->{ID} == $_[0]->{ID} },
#     '!='   => sub { ! ref($_[1]) || $_[1]->{ID} != $_[0]->{ID} },
#     fallback => 1;

sub new {
    my ($pkg, %args) = @_;

    my $data = $args{data};

    return bless {
        ID             => $args{ID},

        data           => $data,
        has_first_save => $args{has_first_save} || 0,
        key_size       => $args{key_size},
        store          => $args{store},
        table          => $args{table},
        table_label    => $args{table_label},
        type           => $args{type},
        value_type     => $args{value_type},
    }, $pkg;
}

# does this object match the given type
sub is_type {
    my ($self, $expected_type) = @_;
    my $type = $self->{type};

    # if an anything reference, any reference type matches
    return 1 if $expected_type eq '*';

    return $type eq $expected_type;
}


sub has_first_save {
    return shift->{has_first_save};
}

#
# Instance methods
#
sub id {
    return shift->{ID};
}

# returns this for caching (tied things return their tied data structures)
sub cache_obj {
    shift;
}

sub data {
    return shift->{data};
}

sub store {
    return shift->{store};
}

sub dirty {
    my $self = shift;
    $self->store->dirty( $self );
}

1;
