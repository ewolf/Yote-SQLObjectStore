#!/usr/bin/perl
use strict;
use warnings;
no warnings 'uninitialized';

use lib './t/lib';
use lib './lib';

use Yote::SQLObjectStore::MariaDB;
use Yote::SQLObjectStore::MariaDB::TableManagement;

use Data::Dumper;
use File::Temp qw/ :mktemp tempdir /;
use Test::Exception;
use Test::More;

use Tainer;
use NotApp;

my %args = (
    user     => 'wolf',
    password => 'B00gerzaais',
    host     => 'localhost',
    port     => 3306,
    );
my $factory = Factory->new( %args );

$factory->setup;

done_testing;
exit;

package Factory;

use Yote::RecordStore;
use File::Temp qw/ :mktemp tempdir /;

sub new_db_name {
    my ( $self ) = @_;
    my $dir = tempdir( CLEANUP => 1 );
    return $dir;
} #new_db_name

sub new {
    my ($pkg, %args) = @_;
    return bless { args => {%args}, dbnames => {} }, $pkg;
}

sub new_db_handle {
    my ($self) = @_;

    # make a test db
    my $dir = $self->{args}{directory} = $self->new_db_name;
    return $dir;
}
sub teardown {
    my $self = shift;
}
sub setup {
    my $self = shift;
}
