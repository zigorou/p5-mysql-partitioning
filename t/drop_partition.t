use strict;
use warnings;
use lib 't/lib';

use Test::More;
use Test::Exception;
use Test::MySQL::Partitioning;

use MySQL::Partitioning;

my $mysqld   = Test::MySQL::Partitioning->setup(0);
my $dbh_args = [
    $mysqld->dsn( dbname => 'test_partitioning' ),
    'root', '', +{ AutoCommit => 1, RaiseError => 1 }
];

sub test_drop_partition {
    my %specs = @_;
    my ( $input, $expects, $desc ) = @specs{qw/input expects desc/};
    my ( $dbh, $table, $partition_name ) =
      @$input{qw/dbh table partition_name/};

    subtest $desc => sub {
        my $p = MySQL::Partitioning->new(
            dbh         => $dbh,
            user        => 'root',
            credentical => '',
            table       => $table,
        );

        Test::MySQL::Partitioning->setup_tables( $p->dbh );

        lives_ok { $p->drop_partition($partition_name) }
        'drop_partition() lives ok';
        $p->load;
        ok(
            !exists $p->partitions_map->{$partition_name},
            sprintf( 'partition %s is dropped', $partition_name )
        );
        is( scalar @{$p->partitions}, $expects, sprintf('partition length: %d', $expects) );
        Test::MySQL::Partitioning->teardown_tables( $p->dbh );

        done_testing;
    };
}

test_drop_partition(
    desc  => 'table: employee2, partition_name: pCentral',
    input => +{
        dbh            => $dbh_args,
        table          => 'employees2',
        partition_name => 'pCentral'
    },
    expects => 3,
);

test_drop_partition(
    desc  => 'table: employee3, partition_name: p3',
    input => +{
        dbh            => $dbh_args,
        table          => 'employees3',
        partition_name => 'p3'
    },
    expects => 3,
);

test_drop_partition(
    desc  => 'table: activities, partition_name: p20100914',
    input => +{
        dbh            => $dbh_args,
        table          => 'activities',
        partition_name => 'p20100914'
    },
    expects => 1,
);

done_testing;

# Local Variables:
# mode: perl
# perl-indent-level: 4
# indent-tabs-mode: nil
# coding: utf-8-unix
# End:
#
# vim: expandtab shiftwidth=4:
