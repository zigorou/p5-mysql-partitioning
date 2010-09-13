package Test::MySQL::Partitioning;

use strict;
use warnings;
use Test::Requires qw(
  Test::mysqld
);
use Carp;
use DBI;
use Path::Class;
use SQL::SplitStatement;

our $VERSION = '0.01';

sub setup {
    my ( $class, $do_setup_tables ) = @_;

    $do_setup_tables = 1 unless defined $do_setup_tables;
    
    my $mysqld = Test::mysqld->new(
        my_cnf => +{
            'skip-networking' => '',
        },
    );

    my $dbh = DBI->connect($mysqld->dsn, 'root', '', +{ AutoCommit => 0, RaiseError => 1, });
    $dbh->do('CREATE DATABASE test_partitioning') or croak($dbh->errstr);
    $dbh->do('USE test_partitioning') or croak($dbh->errstr);

    if ( $do_setup_tables ) {
        $class->setup_tables( $dbh );
    }
    
    $dbh->disconnect;
    
    return $mysqld;
}

sub setup_tables {
    my ( $class, $dbh ) = @_;
    my $ss = SQL::SplitStatement->new(+{
        keep_terminator => 1,
        keep_extra_spaces => 1,
        keep_comments => 1,
        keep_empty_statements => 0,
    });

    my $sql = file(__FILE__)->parent->subdir('..', '..', '..', 'sql')->file('tables.sql')->slurp;

    for my $stmt ( $ss->split($sql) ) {
        $dbh->do( $stmt ) or croak($dbh->errstr);
    }
}

sub teardown_tables {
    my ( $class, $dbh ) = @_;

    my $rs = $dbh->selectall_arrayref( 'SHOW TABLES' );
    for my $table ( map { $_->[0] } @$rs ) {
        $dbh->do( 'DROP TABLE ' . $table );
    }
}

1;

__END__

=head1 NAME

Test::MySQL::Partitioning - write short description for Test::MySQL::Partitioning

=head1 SYNOPSIS

  use Test::MySQL::Partitioning;

=head1 DESCRIPTION

=head1 METHODS

=head1 AUTHOR

Toru Yamaguchi E<lt>zigorou@dena.jp<gt>

=head1 LICENSE

This module is licensed under the same terms as Perl itself.

=head1 SEE ALSO

=cut

# Local Variables:
# mode: perl
# perl-indent-level: 4
# indent-tabs-mode: nil
# coding: utf-8-unix
# End:
#
# vim: expandtab shiftwidth=4:
