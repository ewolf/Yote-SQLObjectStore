package Yote::SQLObjectStore::SQLite;

use base 'Yote::SQLObjectStore::Base';

sub connect_sql {
    my ($pkg,%args) = @_;
    
    my $file = $args{file} or die __PACKAGE__."::connect_sql requires 'file' argument";
    
    my $dbh = DBI->connect( "DBI:SQLite:dbname=$file", 
                            undef, 
                            undef, 
                            { PrintError => 0 } );
    die "$@ $!" unless $dbh;
    return $dbh;
    
}
