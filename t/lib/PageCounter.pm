package PageCounter;

use strict;
use warnings;

use Yote::App;
use base 'Yote::App';

sub update_counter {
    my( $self, $lucky ) = @_;
    $self->lock( "PAGECOUNTER" );

    $self->get__secret_count(10);
          
    if( int($self->get_hits(0)/10) == $self->get_hits/10 ) {
       $self->set__secret_count( 1 + $self->get__secret_count );
    }

    $self->vol( 'lucky_number', $lucky ) if $lucky;
    
    return 1, $self->set_hits( 1 + $self->get_hits(0) );
}

sub make_reset_link {
    my( $suc, $link ) = shift->_make_reset_link( @_ );
    return 1, $link;
}

sub update {
    return 1;
}

1;
