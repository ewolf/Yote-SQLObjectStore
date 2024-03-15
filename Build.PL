use strict;
use warnings;
use Module::Build;


my $builder = Module::Build->new(
    add_to_cleanup      => [ 'Yote-ObjectStore*' ],
    build_requires => {
        'Test::Exception' => 0,
        'Test::More'      => 0,
        'Module::Build'   => 0.40,
    },
    dist_abstract      => 'Yote',
    create_makefile_pl => 'traditional',
    license            => 'perl',
    module_name        => 'Yote::SQLObjectStore',
    release_status     => 'stable',
    test_files         => [ 't/object_store_sqlite.t',
                            't/object_store_mariadb.t',
                        ],
    requires => {
        'perl'            => 'v5.16',
        'DBD::MariaDB'    => 'v1.23',
        'DBD::SQLite'     => 'v1.74',
        'DBI'             => 0,
        'File::Grep'      => 0,
        'Set::Scalar'     => 0,
    },
    );

$builder->create_build_script();
