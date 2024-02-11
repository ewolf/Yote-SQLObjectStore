package Widget;

use strict;
use warnings;

use NotApp;

use Yote::Obj;
use base 'Yote::Obj';

sub hi {
    return 1, "hi there";
}
sub name {
    return 1, shift->get_name;
}

sub badsend {
    return 1, NotApp->new;
}

sub attachstuff {
    my $self = shift;
    $self->set_arry( [ "ONE", { TWO => "TREE" } ] );
    $self->vol( "STUFF", "KINDA STUFF" );
    return 1, "ATTACHED";
}

1;

