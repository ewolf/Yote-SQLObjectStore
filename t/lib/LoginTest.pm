package LoginTest;

use strict;
use warnings;

use Yote::App;
use base 'Yote::App';

sub _init {
    my $self = shift;
    $self->SUPER::_init;
    $self->set__toggler( $self->store->new_obj( 'Widget' ) );
}

sub hello {
    return 1, "HELLO WORLD";
}

sub echo {
    my( $self, $args, $sess ) = @_;
    my $login = $sess->get_login;
    if( $login ) {
        return 1, "Hello $args";
    }
    return 0, 'must be logged in';
}

sub reign {
    my( $self, $args, $sess ) = @_;
    my $login = $sess->get_login;
    if( $login && $login->get_is_admin ) {
        return 1, "My Leige";
    }
    return 0, 'must be logged in';
    
}

sub toggle {
    my $self = shift;
    if( $self->get_toggler ) {
        $self->set__toggler( $self->get_toggler );
        $self->remove_field( 'toggler' );
    } else {
        $self->set_toggler( $self->get__toggler );
        $self->remove_field( '_toggler' );
    }
    return 1, "toggled";
}

sub give_arry {
    return 1, [ 'here are', 'return values' ];
}

sub give_hash {
    return 1, { what => "DO YOU", want => "TO SAY" };
}

sub give_amix {
    return 1, [ { this => "ISAHASH" } ];
}

sub give_hmix {
    return 1, { arry => [1,2,3,4] };
}

sub array_in {
    my( $self, $args, $sess ) = @_;
    return 1, join( ' ', @$args );
}

sub make_widget {
    my( $self, $args, $sess ) = @_;
    my $widg = $self->store->new_obj( 'Widget' );
    $widg->set_name( $args);
    return 1, $widg;
}

sub widget_set {
    my( $self, $widg, $sess ) = @_;
    $self->set__cur_widget( $widg );
    return 1, $widg;
}

sub widget_geta {
    my( $self, $args, $sess ) = @_;
    my $w = $self->get__cur_widget;
    if( $w ) {
        return 1, [$w,{widget => $w}];
    }
    return 1;
}

sub widget_geth {
    my( $self, $args, $sess ) = @_;
    my $w = $self->get__cur_widget;
    if( $w ) {
        return 1, {widget => [$w]};
    }
    return 1;
}


1;
