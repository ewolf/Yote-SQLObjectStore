use strict;
use warnings;
use Module::Build;


my $builder = Module::Build->new(
    add_to_cleanup      => [ 'Yote-ObjectStore*' ],
    build_requires => {
        'Test::More' => 0,
        'Module::Build' => 0.40,
    },
    dist_abstract      => 'Yote',
    create_makefile_pl => 'traditional',
    license            => 'perl',
    module_name        => 'Yote::ObjectStore',
    release_status     => 'stable',
    test_files         => [ 't/object_store_sqlite.t',
                            't/object_store_mariadb.t',
                        ],
    requires => {
        'perl'             => 'v5.16',
        'Yote::RecordStore' => 0.10,
    },
    );

$builder->create_build_script();
