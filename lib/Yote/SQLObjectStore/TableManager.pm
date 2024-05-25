package Yote::SQLObjectStore::TableManager;

use 5.16.0;
use warnings;

use File::Grep qw(fgrep fmap fdo);
use Module::Load::Conditional qw(requires can_load);
use Set::Scalar;

sub new {
    my ($pkg, $store) = @_;
    return bless { store => $store}, $pkg;
}

sub walk_for_perl {
    my ($self, $base_obj_pkg, $seen_packages, $root, @path) = @_;

    my @mods;

    my $path = join( '/', $root, @path );

    my @perls = map { my $fn = $_->{filename}; $fn =~ s/(.*\/)([^\/]*).pm/$2/; join( "::", @path, $fn ) }
                grep { $_->{count} } 
                fgrep { /Yote::.*::Obj/ } 
                glob "$path/*pm";

    my %files = reverse %INC;

    for my $mod (@perls) {
        my $as_path = join( '/', split( /::/, $mod)) . '.pm';
        next if $files{$as_path};
        next if $seen_packages->{$mod}++;
        my @reqlist = requires( $mod );
        if (grep { $_ eq $base_obj_pkg } @reqlist) {
            push @mods, $mod;
        }
    }

    # check for subdirs
    opendir my $dh, $path or return;

    for my $file (grep { $_ !~ /^..?$/ } readdir($dh)) {
        if( -d "$path/$file" ) {
            push @mods, $self->walk_for_perl( $base_obj_pkg, $seen_packages, $root, @path, $file );
        }
    }
    return @mods;
}

sub find_obj_packages {
    my ($self,$base_obj_package) = @_;
    my @mods;
    my $seen_packages = {};
    for my $dir (@INC) {
        next if $dir eq '.';
        # find the perl files in this directory
#print STDERR ">>CHECK>>$dir\n";
        push @mods, $self->walk_for_perl( $base_obj_package, $seen_packages, $dir );
    }

    return @mods;
}

sub label_to_table {
    my ($self, $label) = @_;
    if ($label =~ /^\*HASH<(\d+)>_(.*)/) {
        my ($key_size, $val_type) = ($1, $2);
        if ($val_type =~ /^\*/) {
            return "HASH_${key_size}_REF";
        }
        $val_type =~ s/[()]/_/g;
        return "HASH_${key_size}_$val_type";
    }
    elsif ($label =~ /^\*ARRAY_\*/) {
        return "ARRAY_REF";
    }
    elsif ($label =~ /^\*ARRAY_(.*)/) {
        my $array_type = $1;
        $array_type =~ s/[()]/_/g;
        return "ARRAY_$array_type";
    }
    return $label;
}

sub generate_hash_table {
    my ($self, $container_type, $field_type) = @_;

    my $tables = $self->{tables};

    my $table_label = join '_', $container_type, $field_type;

    return if $tables->{$table_label};

    my $alias_of;

    my ($key_size) = ( $container_type =~ /<(\d+)>/ );

    my @column_sql = ( 
        "id BIGINT UNSIGNED",
        "hashkey VARCHAR($key_size)",
        );

    my $table_name = $self->label_to_table( $table_label );
    if ($field_type =~ /^\*(.*)/) {
        $alias_of = "*HASH<${key_size}>_*";
        $self->generate_reference_table( $1 );
        push @column_sql, "val BIGINT UNSIGNED";
    } else {
        push @column_sql, "val $field_type";
    }

    # alias definition
    my $table_def = $tables->{$table_label} = {
        field_type   => $field_type, #maybe yes, maybe no
    };

    push @column_sql, "UNIQUE (id,hashkey)";
    my $create_table_sql = "CREATE TABLE IF NOT EXISTS $table_name (" .
        join( ',', @column_sql ) .')';    
    #
    # if the table is a value array, its name and alias will be
    # the same, so add the create_table field.
    # 
    if ($alias_of && $alias_of ne $table_label) {
        $table_def->{alias_of} = $alias_of;
        $self->generate_hash_table( $container_type, '*' );
    } else {
        $table_def->{table_name} = $table_name,
        $table_def->{create_table_sql} = $create_table_sql;
    }
}

sub generate_array_table {
    my ($self, $field_type) = @_;
    
    # generating a virtual table data structure
    # representing an array that has column reference
    # constraints
    #
    # the array items may be references or values.
    #
    # if values, they are a sql type like varchar(123) or tinyint
    #
    # if references, they are an integer that points to the ARRAY_REF
    # table, but the particular perl array object does type checking
    # using this data structure
    #
    # the table has a label to identify it. if it is a typed reference
    # it has an alias_of field that points to the table label ARRAY_REF
    #
    my $tables = $self->{tables};

    my $table_label = join '_', '*ARRAY', $field_type;
    return if $tables->{$table_label};

    my $alias_of;

    my @column_sql = ( 
        "id BIGINT UNSIGNED",
        "idx INT UNSIGNED",
        );

    my $table_name = $self->label_to_table( $table_label );
    
    if ($field_type =~ /^\*(.*)/) {
        $alias_of = "*ARRAY_*";
        $self->generate_reference_table( $1 );
        push @column_sql, "val BIGINT UNSIGNED";
    } else {
        push @column_sql, "val $field_type";
    }

    my $table_def = $tables->{$table_label} = {
        field_type   => $field_type, #maybe yes, maybe no
    };

    push @column_sql, "UNIQUE (id,idx)";
    my $create_table_sql = "CREATE TABLE IF NOT EXISTS $table_name (" .
        join( ',', @column_sql ) .')';    

    #
    # if the table is a value array, its name and alias will be
    # the same, so add the create_table field.
    # 
    if ($alias_of && $alias_of ne $table_label) {
        $table_def->{alias_of} = $alias_of;
        $self->generate_array_table( '*' );
    } else {
        $table_def->{table_name} = $table_name,
        $table_def->{create_table_sql} = $create_table_sql;
    }
}

sub generate_reference_table {
    my ($self, $col_type) = @_;

    if ($col_type =~/^\*((ARRAY|HASH(<\d*>)?)_)?(.*)/) {
        if ($2 && $2 eq 'ARRAY') {
            $self->generate_array_table( $4 );
        } elsif( $2 ) {
            $self->generate_hash_table( "*$2", $4 );
        } elsif ($3) {
            $self->generate_table_from_module( $3 );
        }
    }
}

sub generate_table_from_module {
    my ($self, $mod) = @_;
    my $tables = $self->{tables};
    my $table_label = $mod;

    my $table_name = join '_', reverse split /::/, $mod;

    return if $tables->{$table_label};

    if (! can_load( modules => { $mod => 0 }, verbose => 1 )) {
        die "unable to load module '$mod'";
    }

    my @column_sql = ( "id BIGINT UNSIGNED PRIMARY KEY" );
    my %column_defs;

    my $cols = $mod->cols;
    for my $col_name (keys %$cols) {
        my $col_type = $cols->{$col_name};
        if ($col_type =~ /^\*/) {
            # a reference
            $self->generate_reference_table( $col_type );
            push @column_sql, "$col_name BIGINT UNSIGNED";
        } else {
            # a scalar
            push @column_sql, "$col_name $col_type";
        }
    }

    my $create_table_sql = "CREATE TABLE IF NOT EXISTS $table_name (" .
        join( ',', @column_sql ) .')';

    $tables->{$table_name} = {
        module           => $mod,
        table_name       => $table_name,
        create_table_sql => $create_table_sql,
        column_defs      => $cols,
    };
    $create_table_sql;
}

sub generate_tables_sql {
    my ($self, $base_obj_package) = @_;
    
    my $tables = $self->{tables} = {}; #  table name -> { table data }
    my @mods = $self->find_obj_packages( $base_obj_package );

    for my $mod (@mods) {
        $self->generate_table_from_module( $mod );
    }

    # now we have table definitions and sql
    my @sql = (
        [$self->create_object_index_sql],
        [$self->create_table_defs_sql],
        [$self->create_table_versions_sql],
        );
    push @sql, $self->tables_sql_updates;
    @sql;
}

sub tables_sql_updates {
    my $self = shift;
    my $tables = $self->{tables};
    my $store = $self->{store};

    my @sql;
    for my $table (values %$tables) {
        if (my $create = $table->{create_table_sql}) {
            next if $table->{was_generated}++;
            my $table_name = $table->{table_name};

            my $version;
            my $needs_new_version = 1;

            my ($has_tables) = $store->has_table('TableVersions');
            if ($has_tables) {
                my ($count) = $store->query_line( "SELECT COUNT(*) FROM TableVersions WHERE name=? AND create_table=?", $table_name, $create );
                if ($count > 0) {
                    $needs_new_version = 0;
                    ($version) = $store->query_line( "SELECT MAX(version) FROM TableVersions WHERE name=?", $table_name );
                    my ($old_table) = $store->query_line( "SELECT create_table FROM TableVersions WHERE name=? AND version=?", $table_name, $version );
                    if ($old_table) {
                        
                        # extract and compare the columns
                        my ($old_columns_defs) = ($old_table =~ /^[^(]*\([^)]+\)/s);
                        my $old_columns = Set::Scalar->new;
                        my %old_columns;
                        for my $col (split ',', $old_columns_defs) {
                            $old_columns->insert( $col );
                            my ($name, $def) = split /\s+/, $col, 2;
                            $old_columns{$name} = $def;
                        }
                        my ($new_columns_defs) = ($create =~ /^[^(]*\([^)]+\)/s);
                        my $new_columns = Set::Scalar->new;
                        my %new_columns;
                        for my $col (split ',', $new_columns_defs) {
                            $new_columns->insert( $col );
                            my ($name, $def) = split /\s+/, $col, 2;
                            $new_columns{$name} = $def;
                        }

                        my $signatures_uniq_to_new = $new_columns->difference($old_columns);
                        my $signatures_uniq_to_old = $old_columns->difference($new_columns);

                        my %seen;

                        for my $col (@$signatures_uniq_to_new) {
                            # columns to add or change
                            my ($col_name, $col_def) = split /\s+/, $col, 2;
                            $seen{$col_name}++;

                            # this column is changed
                            if ($old_columns{$col_name}) {
                                # update the column. is a list. for example sqlite needs more than one sql commands
                                # to change a column
                                my @update_sql = $self->change_column( $table_name, $col_name, $col_def );
                                push @sql, @update_sql;
                            } 
                            else { 
                                #this column is new
                                my @new_sql = $self->new_column( $table_name, $col_name, $col_def );
                                push @sql, @new_sql;
                            }
                        }

                        for my $col (@$signatures_uniq_to_old) {
                            # columns to archive
                            my ($col_name, $col_def) = split /\s+/, $col, 2;
                            my @new_sql = $self->archive_column( $table_name, $col_name );
                            push @sql, @new_sql;

                        }
                    }
                } else {
                    # brand new table;
                    push @sql, [$create];
                }
            } else {
                # brand new table;
                push @sql, [$create];
            }
            if ($needs_new_version) {
                $version = 1 + ($version // 0);
                push @sql, ["INSERT INTO TableVersions (name,version,create_table) VALUES (?,?,?)",
                            $table_name, $version, $create ];
                push @sql, ["INSERT IGNORE INTO TableDefs (name,version) VALUES (?,?)", $table_name, $version ];
            }
        }
    }

    @sql;
}

1;
