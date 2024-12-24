#!/usr/bin/perl
use strict;
use warnings;
no warnings 'uninitialized';

use lib './t/lib';
use lib './lib';

use Data::Dumper;
use File::Temp qw/ :mktemp tempdir /;
use Scalar::Util qw(weaken blessed);
use Test::Exception;
use Test::More;

use Yote::SQLObjectStore;

sub sqlstr {
    my $pair = shift;
    my ($sql, @qparams) = @$pair;
    while (my $qp = shift @qparams) {
        $sql =~ s/\?/'$qp'/s;
    }
    print STDERR "$sql\n\n";
}

my %args = (
    username => 'wolf',
    password => 'boogers',
    host     => 'localhost',
    port     => 3306,
    );
my $factory = Factory->new( %args );

$factory->setup;

if(0){
    my $db_handle = $factory->new_db_handle;
    my $object_store = Yote::SQLObjectStore::MariaDB->new(
        dbname => $db_handle,
        %args,
        );
    my @sql = $object_store->make_all_tables_sql;
    for my $sql (@sql) {
        sqlstr( $sql );
    }
    pass "barf";
    done_testing;
    exit;
}

sub make_all_tables {
    my $object_store = shift;
    local @INC = qw( ./lib ./t/lib );
    $object_store->make_all_tables;
    
}

subtest 'reference and reopen test' => sub {
    my $db_handle = $factory->new_db_handle;

    {
        my $object_store = Yote::SQLObjectStore->new( 'MariaDB', 
            dbname => $db_handle,
            %args,
            );
        make_all_tables( $object_store );
        $object_store->open;
        is ($object_store->record_count, 3, 'root record and its hashes in store');

        my $r1 = $object_store->fetch_root;
        ok( $r1, 'got a root');
        my $r2 = $object_store->fetch_root;
        is ($r1, $r2, "single reference for fetch root" );
        ok ($r1 == $r2, "same reference compared numerically" );
        ok (!($r1 != $r2), "same reference compared as not equal numerically" );
        ok (!($r1 == 1), "reference does not equal to a number" );
        ok ($r1 != 1, "reference is not equal to a number" );
        ok ($r1 eq $r2, "same reference compared as strings" );
        ok (!($r1 ne $r2), "same reference compared as not equal strings" );
        ok ($r1 != 1, "not equal to a number" );
        ok (! ($r1 eq 1), "not equal to a string" );
        ok ($r1 ne 1, "not the same as a number" );

        ok (! $object_store->is_dirty( 1234 ), 'numbers cannot be dirty' );
        my $nothing = bless { }, 'MariaDB::NotAThing';
        ok (! $object_store->is_dirty( $nothing ), 'non base objects cannot be dirty' );

        my $root_vals_hash = $r1->get_val_hash;
        
        is_deeply( $root_vals_hash, {}, 'val hash starts empty' );

        $root_vals_hash->{foo} = 'bar';
        $root_vals_hash->{bar} = 'gaz';
        
        is_deeply( $root_vals_hash, { foo => 'bar', bar => 'gaz'}, 'val hash with stuff in it' );

        # now gotta get stuff in the ref, like a [], {} and obj
        my $root_refs = $r1->get_ref_hash;

        # make some object too
        my $wilma = $object_store->new_obj( 'MariaDB::SomeThing', name => 'wilma' );
        my $brad = $object_store->new_obj( 'MariaDB::SomeThing', name => 'brad', sister => $wilma  );

        # make some data structures to put in root ref hash
        my $val_arry = $object_store->new_array( '*ARRAY_VARCHAR(234)', 1,2,3 );

        $root_refs->{val_array} = $val_arry;
        my $ref_arry = $root_refs->{ref_array} = $object_store->new_array( '*ARRAY_*', $r1, $wilma, $brad );
        my $val_hash = $root_refs->{val_hash} = $object_store->new_hash( '*HASH<256>_VARCHAR(233)', a => 1, b => 2, c => 3 );

        my $ref_hash = $root_refs->{ref_hash} = $object_store->new_hash( '*HASH<256>_*', root => $r1);

        my $mty = $root_refs->{empty_hash} = $object_store->new_hash( '*HASH<256>_VARCHAR(231)' );

        $mty->{fooz} = 'barz';
        is_deeply( $root_refs->{empty_hash}, { fooz => 'barz' }, 'filled ref hash' );
        delete $mty->{fooz};
        is_deeply( $root_refs->{empty_hash}, {}, 'empty ref hash' );

        is ($brad->get_name, 'brad', 'brad name' );
        is ($brad->get_sister, $wilma, 'brad sister is wilma' );
        is_deeply( $val_hash, { a => 1, b => 2, c => 3 }, 'val hash' );
        is_deeply( $ref_hash, { root => $r1 }, 'ref hash' );
        is_deeply( $val_arry, [1,2,3], 'val array' );
        is_deeply( $ref_arry, [$r1, $wilma, $brad ], 'ref array' );
        is_deeply( $mty, {}, 'empty ref hash' );

        # data now looks like this
        #   /val_hash/foo -> bar
        #   /val_hash/bar -> gaz
        #   /ref_hash/val_array -> [1,2,3]
        #   /ref_hash/ref_array -> [root,wilma,brad]
        #   /ref_hash/val_hash/ {a =>1,b => 2, c => 3 }
        #   /ref_hash/ref_hash/root -> root
        #   /ref_hash/empty_hash -> {}

        $object_store->save;
    }

    {
        # reopen and make sure its the same stuff
        my $object_store = Yote::SQLObjectStore->new( 'MariaDB', 
            dbname => $db_handle,
            %args,
            );
        $object_store->open;


        is ($object_store->fetch_path( "/val_hash/foo" ), 'bar', 'fetched value' );
        is ($object_store->fetch_path( "/val_hash/bar" ), 'gaz', 'fetched value' );
        is_deeply ($object_store->fetch_path( "/ref_hash/val_array" ), [1,2,3], 'fetched array ref' );


        my $root = $object_store->fetch_root;
        my $root_vals = $root->get_val_hash;
        is_deeply( $root_vals, { foo => 'bar', bar => 'gaz'}, 'val hash with stuff in it after reopen' );
        $root_vals->{zork} = 'money';

        my $root_refs = $root->get_ref_hash;
        my @refs = @{$root_refs->{ref_array}};
        is (@refs, 3, '3 refs' );
        my ($loaded_root, $wilma, $brad) = @refs;
        is( $loaded_root, $root, 'roots are roots' );
        is ($brad->get_sister, $wilma, 'brad sister is wilma' );
        is ($brad->get_name, 'brad', 'brad name' );
        is_deeply( $root_refs->{val_array}, [1,2,3], 'val array' );
        is_deeply( $root_refs->{ref_array}, [$root, $wilma, $brad ], 'ref array' );
        is_deeply( $root_refs->{val_hash}, { a => 1, b => 2, c => 3 }, 'val hash' );
        is_deeply( $root_refs->{ref_hash}, { root => $root }, 'ref hash' );
        is_deeply( $root_refs->{empty_hash}, {}, 'empty ref hash' );

        ok ($object_store->has_id($root_refs), 'hash has an id');

        is ($object_store->fetch_path( "/ref_hash/ref_array" ), $root_refs->{ref_array}, 'fetched path containing array' );
        is ($object_store->fetch_path( "/ref_hash/ref_array/2" ), $brad, 'fetched path containing array' );
        is ($object_store->fetch_path( "/ref_hash/ref_array/2/sister" ), $wilma, 'fetched path containing array and reference' );
        is ($object_store->fetch_path( "/ref_hash/ref_array/2/name" ), 'brad', 'fetched path containing array and value' );
        $brad->set_name( 'new brad' );
        is ($brad->get_name, 'new brad', 'brad new name' );

        my $mth = $object_store->fetch_path( "/ref_hash/empty_hash" );
        my $mtth = $mth;
        is_deeply ($mtth, {}, 'fetched path containing empty hash' );
        $mth->{NOTEMPTY} = 'anymore';
        is_deeply( $mtth, { NOTEMPTY => 'anymore' }, 'newly filled formly empty hash' );
        ok ( $object_store->is_dirty( $mth ), 'no longer empty hash is dirty because stuff was put into it' );
        
        $mth->{NOTEMPTY} = undef;
        is_deeply( $mtth, { NOTEMPTY => undef}, 'hash with an undef value' );
        delete $mtth->{NOTEMPTY};
        is_deeply( $mtth, {}, 'empty formly empty hash was deleted again' );
        $object_store->save;
        ok ( !$object_store->is_dirty( $mth ), 'empty hash no longer dirty after save' );
        is_deeply( $mtth, {}, 'back to empty hash' );


        my $bad = $object_store->new_obj( 'MariaDB::SomeThing', name => 'bad', sistery => $wilma  );
        is ($bad->get_sister, undef, 'bad has no sister' );
        $bad->set_something( $bad );
        is ($bad->get_something, $bad, 'bad is its own something' );
        ok ($object_store->has_id($bad), 'yote obj has an id');

        my $dork = bless { dork => "me" }, 'PKG';
        ok (!$object_store->has_id($dork), 'non yote blessed has no id');


        # give bad a "sister" that is an array ref and "brother" that is array vals
        $bad->set_sister( $object_store->new_array('*ARRAY_*') );
        $bad->set_brother( $object_store->new_array('*ARRAY_VARCHAR(100)') );
        $bad->set_some_ref_array( $bad->get_sister );
        $bad->set_some_val_array( $bad->get_brother ); 
        my $bad_ref_hash = $bad->set_some_ref_hash( $object_store->new_hash('*HASH<256>_*') );
        my $bad_val_hash_obj = $bad->set_some_val_hash( $object_store->new_hash('*HASH<256>_VARCHAR(100)') );
        my $bad_val_hash = $bad_val_hash_obj;
        $bad_val_hash->{LEEROY} = 'brown';
        is( $bad->get_tagline( "TAGGY" ), "TAGGY", 'set via default get' );
        is_deeply ($bad->get_some_ref_array, [], 'bad ref array is empty array' );
        is_deeply ($bad->get_some_val_array, [], 'bad ref array is empty array' );

        is_deeply( $bad->fields, [sort qw(
                                      brother
                                      name
                                      lolov
                                      sister
                                      sisters
                                      sisters_hash
                                      some_ref_array
                                      some_ref_hash
                                      some_val_array
                                      some_val_hash
                                      something
                                      tagline
                                  )],
                   'fields for bad and all SomeThing refs' );
        is( $bad->table_name, 'SomeThing', 'table name for SomeThing refs' );

        push @{$root_refs->{ref_array}}, $bad;
        push @{$root_refs->{ref_array}}, undef;
        $root_refs->{ref_hash}{wilma} = $wilma;


        is_deeply( $object_store->fetch_path( "/ref_hash/ref_array/3" ), $bad, 'fetched path containing bad guy' );
        is_deeply( $object_store->fetch_path( "/ref_hash/ref_array/3/name" ), 'bad', 'fetched path containing bad guy name' );
        is_deeply( $object_store->fetch_path( "/ref_hash/ref_array/3/some_ref_hash" ), {}, 'fetched path containing hash ref in obj' );
        is_deeply( $object_store->fetch_path( "/ref_hash/ref_array/3/some_val_array" ), [], 'fetched path containing array ref in obj' );
        is_deeply( $bad_val_hash, { LEEROY => 'brown' }, 'bad val hash' );
        is_deeply( $object_store->fetch_path( "/ref_hash/ref_array/3/some_val_hash" ), { LEEROY => 'brown' }, 'fetched path containing value hash' );
        is( $object_store->fetch_path( "/ref_hash/ref_array/3/some_val_array/1" ), undef, 'fetched path to non extant index in array' );

        is ($object_store->fetch_path( "/ref_hash/ref_array/3/some_ref_hash/nothere" ), undef, 'nothing to see here is undef' );

        is ($object_store->fetch_path( "/ref_hash/ref_array/4" ), undef, 'last entry in ref array is undef' );

        $object_store->save;

        $wilma->set_tagline( "its gonna get hotter" );
        $object_store->save( $wilma );


        ok (! $object_store->is_dirty( $bad_val_hash_obj ), 'bad val hash is not dirty here' );
        $bad_val_hash->{LEEROY} = 'brown';
        ok (! $object_store->is_dirty( $bad ), 'bad val hash is still not dirty here' );
        delete $bad_val_hash->{NOTHERE};
        ok (! $object_store->is_dirty( $bad ), 'bad val hash is still not dirty after deleting nothing there' );

        ok (! $object_store->is_dirty( $bad_ref_hash ), 'bad ref hash is not dirty here' );
        %{$bad_ref_hash} = (); #clear it
        ok (! $object_store->is_dirty( $bad_ref_hash ), 'bad ref hash still not dirty here after clearing it remains the same' );

        ok (! $object_store->is_dirty( $bad_val_hash_obj ), 'bad val hash is not dirty here' );
        %$bad_val_hash = (); #clear it
        ok ($object_store->is_dirty( $bad_val_hash_obj ), 'bad val hash is dirty after clearing it' ); 
        # not saving it though, so the clearing wont show up next load


        
    }

    {
        # reopen and make sure its the same stuff
        my $object_store = Yote::SQLObjectStore->new( 'MariaDB', 
            dbname => $db_handle,
            %args,
            );
        $object_store->open;
        my $root = $object_store->fetch_root;
        my $root_vals = $root->get_val_hash;

        is_deeply( $root_vals, { foo  => 'bar', 
                                 zork => 'money',
                                 bar  => 'gaz'}, 'val hash with stuff in it after reopen' );

        my $root_refs = $root->get_ref_hash;

        my $refs = $root_refs->{ref_array};
        is (@$refs, 5, '5 refs with bad and undef' );

        my ($loaded_root, $wilma, $brad, $bad, $undef) = @$refs;
        ok ($undef == 0, 'null  reference stored' );
        is ($brad->get_name, 'new brad', 'brad name still new brad' );
        is ($bad->get_name, 'bad', 'bad name' );
        is_deeply ($bad->get_sister, [], 'bad sister is empty array' );
        is_deeply ($bad->get_brother, [], 'bad brother is empty array' );
        is_deeply ($bad->get_some_ref_array, [], 'bad ref array is empty array' );
        is_deeply ($bad->get_some_val_array, [], 'bad value array is empty array' );
        is_deeply ($bad->get_some_ref_hash, {}, 'bad ref hash is empty' );
        is_deeply ($bad->get_some_val_hash, { LEEROY => 'brown' }, 'bad value hash same values' );
        is ($bad->get_tagline, "TAGGY", 'tag set via default get' );
        is ($bad->get_something, $bad, 'bad is its own something' );
        is ($wilma->get_name, 'wilma', 'wilma name' );
        is ($wilma->get_tagline, 'its gonna get hotter', 'wilma was saved with new tagline' );
        is ($loaded_root, $root, 'roots are roots' );
        is_deeply( $root_refs->{ref_hash}, { root => $root, wilma => $wilma }, 'ref hash' );

        ok (! $object_store->is_dirty( $bad ), 'bad is not dirty here' );
        $bad->set_tagline( "TAGGY" );
        ok (! $object_store->is_dirty( $bad ), 'bad is *still* not dirty here after setting the tagline to what it already was' );


        throws_ok { $bad->set_SDFSDFSDF } qr/No field 'SDFSDFSDF'/, 'bad cannot set a field it does not have';
        ok (! $object_store->is_dirty( $bad ), 'bad is *still* not dirty here after trying to set nonexistant field' );
        throws_ok { $bad->get_SDFSDFSDF } qr/No field 'SDFSDFSDF'/, 'bad cannot set a field it does not have';
        ok (! $object_store->is_dirty( $bad ), 'bad is *still* not dirty here after trying to get nonexistant field' );


        my $bad_val_array = $bad->get_some_val_array;

        is_deeply ($bad_val_array, [], 'bad val array starts empty' );
        ok (! $object_store->is_dirty( $bad_val_array ), 'bad val array starts clean' );        
        my $bad_val_tied_array = $bad_val_array;
        is (@$bad_val_tied_array, 0, 'bad val tied array starts empty' );
        ok (! $object_store->is_dirty( $bad_val_array ), 'bad val array still clean after tied array called' );

        push @$bad_val_tied_array; #pushing nothing
        ok (! $object_store->is_dirty( $bad_val_array ), 'bad val array still clean after empty push' );        
        unshift @$bad_val_tied_array; #pushing nothing
        ok (! $object_store->is_dirty( $bad_val_array ), 'bad val array still clean after empty unshift' );

        pop @$bad_val_tied_array; #popping nothing
        ok (! $object_store->is_dirty( $bad_val_array ), 'bad val array still clean after empty pop' );        
        shift @$bad_val_tied_array; #pushing nothing
        ok (! $object_store->is_dirty( $bad_val_array ), 'bad val array still clean after empty shift' );


        throws_ok { $bad->set_some_ref_array( $bad->get_some_ref_hash ) } qr/incorrect type '\*HASH<256>_\*' for '\*ARRAY_\*'/, 'cannot set a ref hash to a ref array';
        throws_ok { $bad->set_some_ref_hash( $bad->get_some_ref_array ) } qr/incorrect type '\*ARRAY_\*' for '\*HASH<256>_\*'/, 'cannot set a ref array to a hash ref';
        throws_ok { $bad->set_some_val_hash( $bad_val_array ) } qr/incorrect type '\*ARRAY_VARCHAR[(]100[)]' for '\*HASH<256>_VARCHAR[(]100[)]'/, 'cannot set a val array to a val hash';
        throws_ok { $bad->PLUGH } qr/unknown function 'MariaDB::SomeThing::PLUGH'/, 'object autoload does not know PLUGH';
        throws_ok { $bad->set_some_val_hash( "SPOOKEY" ) } qr/incorrect type 'scalar value' for '\*HASH<256>_VARCHAR\(100\)'/, 'cannot set a val array to a val array';

        throws_ok { $bad->set_some_val_hash( $bad->get_some_ref_hash ) } qr/incorrect type '\*HASH<256>_\*' for '\*HASH<256>_VARCHAR[(]100[)]'/, 'cannot set a ref array to a val array';

        my $root_val_array = $root_refs->{val_array};

        $bad_val_array->[100] = "ONEHUND";
        $object_store->save;

        ok (! $object_store->is_dirty( $bad_val_array ), 'bad val array just saved, is not dirty' );
        $bad_val_array->[100] = "ONEHUND";
        ok (! $object_store->is_dirty( $bad_val_array ), 'bad val array still not dirty after setting index to value it already had' );

        is (@{$bad_val_array}, 101, '101 entries for bad val array' );

        ok (! defined $bad_val_array->[99], 'nothing at index 99 inp bad val array' );
        
        ok (! $object_store->is_dirty( $bad_val_array ), 'bad val array still not dirty after checking exists' );
        delete $bad_val_array->[98];
        ok (! $object_store->is_dirty( $bad_val_array ), 'bad val array still not dirty after deleting something that was already undefined' );
        is (@{$bad_val_array}, 101, 'still 101 entries for bad val array' );

        delete $bad_val_array->[100];
        is (@{$bad_val_array}, 100, 'down to 100 entries after deleting the 101st' );
        ok ($object_store->is_dirty( $bad_val_array ), 'bad val array now dirty after deleting only entry' );
        $object_store->save;
        ok (!$object_store->is_dirty( $bad_val_array ), 'bad val array now not dirty after save' );
        @{$bad_val_array} = ();

        $bad_val_array->[100] = 'ONEHUND'; #setting this back
        is (@{$bad_val_array}, 101, 'back to 101 entries for bad val array' );
        $object_store->save;
        ok (!$object_store->is_dirty( $bad_val_array ), 'bad val array now not dirty after save' );
        @{$bad_val_array} = ();
        is (@{$bad_val_array}, 0, 'back to no entries for bad val array' );
        ok ($object_store->is_dirty( $bad_val_array ), 'bad val array now dirty after clearing while empty' );
        # do not save, so ONEHUND is still there
    }

    {
        # reopen and make sure its the same stuff
        my $object_store = Yote::SQLObjectStore->new( 'MariaDB', 
            dbname => $db_handle,
            %args,
            );
        $object_store->open;

        my $bad = $object_store->fetch_path( "/ref_hash/ref_array/3" );
        is_deeply( $object_store->fetch_path( "/ref_hash/ref_array/3/some_val_array/100" ), "ONEHUND", 'fetched path containing indexes array value' );

        my $bad_val_hash_obj = $bad->get_some_val_array;
        
        ok (!$object_store->is_dirty( $bad_val_hash_obj ), 'some val obj not dirty' );
        $bad_val_hash_obj->[100] = 'ONEHUND';
        ok (!$object_store->is_dirty( $bad_val_hash_obj ), 'some val obj still not dirty after setting an index to the value it already is' );


        my $bad_val_array = $bad_val_hash_obj;
        is (@$bad_val_array, 101, '101 entries for bad val array' );
        my $undef = shift @$bad_val_array;
        is ($undef, undef, 'shifted undef value');
        is (@$bad_val_array, 100, '100 entries for bad val array after shift' );

        is_deeply( $object_store->fetch_path( "/ref_hash/ref_array/3/some_val_array/99" ), "ONEHUND", 'fetched path containing indexes array value shifted down one' );
        is ($#$bad_val_array, 99, 'last index 99 for bad val array');
        is ($bad_val_array->[$#$bad_val_array], 'ONEHUND', 'at last');
        is (pop @$bad_val_array, 'ONEHUND', 'ONEHUND POPPED OFF');
        is (@$bad_val_array, 99, 'bad val array now down to 99 size' );
        is ($bad_val_array->[$#$bad_val_array], undef, 'last bad val array entry undef now');

        unshift @$bad_val_array, 'BEGINNING';
        is_deeply( $object_store->fetch_path( "/ref_hash/ref_array/3/some_val_array/0" ), "BEGINNING", 'NOW UNSHIFTED bad val array value' );
        is (@$bad_val_array, 100, 'bad val array now at 100 size after unshift' );

        $object_store->save;
        ok (!$object_store->is_dirty( $bad_val_hash_obj ), 'bad val array now not dirty after save' );
        no warnings 'syntax';
        unshift @$bad_val_array;
        ok (!$object_store->is_dirty( $bad_val_hash_obj ), 'bad val array now not dirty after useless unshift' );

        my $val_array = $object_store->fetch_path( "/ref_hash/val_array" );
        is_deeply ($val_array, [1,2,3], 'val array from fetch path' );
        my (@gone) = splice @$val_array, 1, 1, 'two';
        is_deeply( \@gone, [2], 'spliced away the 2' );
        is_deeply ($val_array, [1,'two',3], 'val array from fetch path' );

        ok ($object_store->has_id($val_array), 'array has an id');

        $#$val_array = 100;
        is (@$val_array, 101, 'val array is now 101 long' );

        ok (!$object_store->has_id, 'undef cant have id');
        ok (!$object_store->has_id("a string"), 'a string cant have id');
        use MariaDB::NotAThing;
        my $nodda = bless {}, 'MariaDB::NotAThing';
        ok (blessed $nodda, 'nota thing is blessed' );
        ok (!$object_store->has_id($nodda), 'root nodda cant have id');
    }

    {
        my $object_store = Yote::SQLObjectStore->new( 'MariaDB', 
            dbname => $db_handle,
            %args,
            );

        $object_store->open;
        my $dbh = $object_store->dbh;
        my $sth = $dbh->prepare( "SHOW CREATE TABLE SomeThing" );
        $sth->execute;
        my $sql = $sth->fetchall_arrayref->[0][1];
        ok ( 0 == index( $sql, 'CREATE TABLE `SomeThing` (
  `id` bigint(20) unsigned NOT NULL,
  `brother` bigint(20) unsigned DEFAULT NULL,
  `lolov` bigint(20) unsigned DEFAULT NULL,
  `name` varchar(100) DEFAULT NULL,
  `sister` bigint(20) unsigned DEFAULT NULL,
  `sisters` bigint(20) unsigned DEFAULT NULL,
  `sisters_hash` bigint(20) unsigned DEFAULT NULL,
  `some_ref_array` bigint(20) unsigned DEFAULT NULL,
  `some_ref_hash` bigint(20) unsigned DEFAULT NULL,
  `some_val_array` bigint(20) unsigned DEFAULT NULL,
  `some_val_hash` bigint(20) unsigned DEFAULT NULL,
  `something` bigint(20) unsigned DEFAULT NULL,
  `tagline` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
)'), 'sql for something table' );


    }
    {

        $MariaDB::SomeThing::cols{newname} = 'VARCHAR(100)';
        delete $MariaDB::SomeThing::cols{brother};
        my $object_store = Yote::SQLObjectStore->new( 'MariaDB', 
            dbname => $db_handle,
            %args,
            );

        make_all_tables( $object_store );
        $object_store->open;

        my $dbh = $object_store->dbh;
        my $sth = $dbh->prepare( "SHOW CREATE TABLE SomeThing" );
        $sth->execute;
        my $sql = $sth->fetchall_arrayref->[0][1];
        ok ( 0 == index( $sql, 'CREATE TABLE `SomeThing` (
  `id` bigint(20) unsigned NOT NULL,
  `brother_DELETED` bigint(20) unsigned DEFAULT NULL,
  `lolov` bigint(20) unsigned DEFAULT NULL,
  `name` varchar(100) DEFAULT NULL,
  `sister` bigint(20) unsigned DEFAULT NULL,
  `sisters` bigint(20) unsigned DEFAULT NULL,
  `sisters_hash` bigint(20) unsigned DEFAULT NULL,
  `some_ref_array` bigint(20) unsigned DEFAULT NULL,
  `some_ref_hash` bigint(20) unsigned DEFAULT NULL,
  `some_val_array` bigint(20) unsigned DEFAULT NULL,
  `some_val_hash` bigint(20) unsigned DEFAULT NULL,
  `something` bigint(20) unsigned DEFAULT NULL,
  `tagline` varchar(200) DEFAULT NULL,
  `newname` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
)'), 'sql for something table' );
    }

};

subtest 'paths test' => sub {

    my $db_handle = $factory->new_db_handle;

    {
        my $object_store = Yote::SQLObjectStore->new( 'MariaDB', 
            dbname => $db_handle,
            %args,
            );

        make_all_tables( $object_store );
        $object_store->open;

        is ( $object_store->fetch_path( '/val_hash/word' ), undef, 'nothing yet in val_hash' );

        is ( ref $object_store->ensure_path( [ 'val_hash' ] ), 'HASH', 'starts with hash' );

        throws_ok
            { $object_store->ensure_path( 'val_hash',
                                          [],
                                          [ 'word', 'VARCHAR(256)' ],
                  ) }
            qr/invalid path. missing key/,
            'ensure_path throws when a key is missing';

        throws_ok
            { $object_store->ensure_path( 'val_hash',
                                          [ 'word', 'VARCHAR(256)' ],
                                          'beep'  ) }
            qr/invalid path. non-reference encountered before the end/,
            'ensure_path throws when there is a non-reference in the middle of the path';

        is ($object_store->fetch_path( '/val_hash/word' ), undef, 'check that bird is not yetthe word' );

        is ($object_store->ensure_path( 'val_hash',
                                        [ 'word', 'bird' ]
            ), 'bird', 'ensure bird is the word' );

        is ($object_store->fetch_path( '/val_hash/word' ), 'bird', 'check that bird is the word' );

        is ($object_store->fetch_path( '/ref_hash/plugh' ), undef, 'ref hash has no plugh' );

        throws_ok
            { $object_store->ensure_path( qw( ref_hash plugh )) }
            qr/requires an object type be given/, 'plugh type not defined';

        throws_ok
            { $object_store->ensure_path( 'ref_hash',
                                          [ 'plugh', 'MariaDB::NotInINC' ] );
            }
            qr/invalid path.*not found in/,
            'plugh cant be set to something not found in @INC';

        throws_ok
            { $object_store->ensure_path( 'ref_hash',
                                          [ 'plugh', 'MariaDB::NotAThing' ] );
            }
            qr/.*is not a Yote::SQLObjectStore::BaseObj/,
            'plugh cant be set to something not a yote obj';


        my $something = $object_store->ensure_path( 'ref_hash',
                                                    [ 'plugh', 'MariaDB::SomeThing' ] );
        ok ($something, 'ensure path returns something' );

        $something = $object_store->ensure_path( '/ref_hash/plugh|MariaDB::SomeThing' );
        ok ($something, 'ensure path returns something' );

        throws_ok
        { $something = $object_store->ensure_path( '/ref_hash/plugh|MariaDB::SomeThingElse' );
        }
        qr/path exists but got type 'MariaDB::SomeThing' and expected 'MariaDB::SomeThingElse'/,
            'ensure path contains an object whos expected class differs';

        throws_ok
        { $object_store->ensure_path( '/val_hash/word|notBird' ); }
        qr/path ends in different value/,
            'ensure path contains a value that differs from the expected value';

        is ($object_store->ensure_path( '/val_hash/word' ), 'bird', 'ensure returns last value as long as its present');
        is ($object_store->ensure_path( '/val_hash/word|bird' ), 'bird', 'ensure returns last value if last value exists');

        my $val_array = $object_store->ensure_path( '/ref_hash/array|*ARRAY_VARCHAR(256)' );
        ok (ref $val_array eq 'ARRAY', 'created value array' );

        throws_ok
        { $object_store->ensure_path( '/ref_hash/array/notanindex|nope' ); }
        qr/array access expects index/,
            'should throw when a non numeric index is used in array';

        $val_array->[0] = "THE FIRST";
        is ($object_store->fetch_path( '/ref_hash/array/0' ), 'THE FIRST', 'fetched value from array' );

        throws_ok
        { $object_store->ensure_path( '/ref_hash/wildcard|*' ); }
        qr/invalid path. wildcard slot requires an object type be given/,
            'should throw when given a wildcard type to instantiate';
        

        my $tiny_hash = $object_store->ensure_path( '/ref_hash/tinyhash|*HASH<3>_VARCHAR(256)' );
        is (ref $tiny_hash, 'HASH', 'made a tiny hash' );
        
        is ($object_store->ensure_path( '/ref_hash/tinyhash/foo|BAR' ), 'BAR', 'made a tiny entry' );

        $object_store->save;

        throws_ok
        { $object_store->ensure_path( '/ref_hash/tinyhash/fooLong|NOWAY' ); }
        qr/key is too large for hash/,
            'should throw when key length exceeds max';

        throws_ok
        { $object_store->ensure_path( '/ref_hash/tinyhash2|*HASH<3>_VARCHAR(256)/fooLonger|STILLNOWAY' ); }
        qr/key is too large/,
            'should throw when key length exceeds max';


        $object_store->ensure_path( '/ref_hash/someobjagain|*MariaDB::SomeThing' );
        

        $object_store->ensure_path( '/ref_hash/somethingHash|*HASH<256>_*MariaDB::SomeThing' );

        my $thing = $object_store->ensure_path( '/ref_hash/somethingHash/afirst|*MariaDB::SomeThing' );
        is (ref $thing, 'MariaDB::SomeThing', 'made something obj' );

        $object_store->save;

        throws_ok
        { $something = $object_store->ensure_path( '/ref_hash/somethingHash/nothere|*MariaDB::SomeThingElse' );
        }
        qr/invalid path. incorrect type '\*MariaDB::SomeThingElse', expected '\*MariaDB::SomeThing'/,
            'type checked hash throws when wrong type added to it';
        

        $object_store->ensure_paths( qw( 
                                         /ref_hash/tinyhash/fuu|BUR
                                         /ref_hash/tinyhash/boo|FAR
                                        ) );
        is ( $object_store->fetch_path( '/ref_hash/tinyhash/boo' ), 'FAR', 'ensure paths worked 1' );
        is ( $object_store->fetch_path( '/ref_hash/tinyhash/fuu' ), 'BUR', 'ensure paths worked 2' );

        throws_ok
        { $something = $object_store->ensure_paths( qw( 
                                         /ref_hash/tinyhash/fu1|GLACK
                                         /ref_hash/tinyhash/bo1|NOOOO
                                         /ref_hash/tinyhash/noway|FAR/gah
                                     ) )
        }
        qr/invalid path. non-reference encountered before the end/,
            'ensure_paths fail for transaction check';
        
        is ( $object_store->fetch_path( '/ref_hash/tinyhash/fu1' ), undef, 'bath paths did nothing 1' );
        is ( $object_store->fetch_path( '/ref_hash/tinyhash/bo1' ), undef, 'bath paths did nothing 2' );
        
    }
};


done_testing;

$factory->teardown;
exit;

package Factory;

use Test::More;
use Yote::SQLObjectStore::MariaDB;

sub new_db_name {
    my ( $self ) = @_;
    return "_test_yote_" . ++$self->{count};
} #new_db_name

sub new {
    my ($pkg, %args) = @_;
    return bless { args => {%args}, dbnames => {} }, $pkg;
}

sub new_db_handle {
    my ($self) = @_;

    # make a test db. fail if it exists
    my $dbh = $self->{dbh};

    my $name = $self->new_db_name;

    my $sth = $dbh->prepare( "SHOW DATABASES LIKE ?" );
    $sth->execute( $name );
    my $existing = $sth->fetchall_arrayref;

    if ($existing && @$existing) {
        BAIL_OUT ("could not create database $name. it exists already" );
    }

    my $x = $dbh->do( "CREATE DATABASE $name" );
    return $name;

}
sub teardown {
    my $self = shift;
    my $dbh = $self->{dbh};
    for (1..$self->{count}) {
        $dbh->do( "DROP DATABASE _test_yote_$_" );
    }
}
sub setup {
    my $self = shift;
    my $dbh = $self->{dbh} = Yote::SQLObjectStore::MariaDB->connect_sql(dbname=>'information_schema',%{$self->{args}});
    if ($dbh) {
        $dbh->do( "DROP DATABASE _test_yote_1" );
        return $self->{dbh} = $dbh;
    }
    die "Unable to run setup";
}
