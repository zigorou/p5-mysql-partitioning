use strict;
use warnings;
use lib 't/lib';

use Test::More;
use Test::Exception;
use Test::MySQL::Partitioning;

use MySQL::Partitioning;

my $mysqld = Test::MySQL::Partitioning->setup;

sub test_partition {
    my ( $got, $expects ) = @_;

    my @fields = qw(
      nodegroup partition_comment partition_description
      partition_expression partition_method partition_name partition_ordinal_position
      subpartition_expression subpartition_method subpartition_name
      subpartition_ordinal_position tablespace_name
    );

    $got = +{ map { $_ => $got->$_() } @fields };

    is_deeply( $got, $expects, 'partition fields' );
}

sub test_get_partition {
    my %specs = @_;
    my ( $input, $expects, $desc ) = @specs{qw/input expects desc/};
    my ( $dbh, $table, $partition_name, $subpartition_name ) =
      @$input{qw/dbh table partition_name subpartition_name/};

    subtest $desc => sub {
        my $p = MySQL::Partitioning->new(
            dbh         => $dbh,
            user        => 'root',
            credentical => '',
            table       => $table,
        );

        my $got;
        lives_ok {
            $got = $p->get_partition( $partition_name, $subpartition_name );
        }
        'get_partition() lives ok';

        test_partition( $got, $expects );

        $p->load;

        is_deeply( $p->get_partition( $partition_name, $subpartition_name ), $got, 'via load' );
        
        done_testing;
    };
}

my $dbh_args = [
    $mysqld->dsn( dbname => 'test_partitioning' ),
    'root', '', +{ AutoCommit => 1, RaiseError => 1 }
];

test_get_partition(
    desc  => 'table k1 (KEY), partition p0',
    input => +{
        dbh               => $dbh_args,
        table             => 'k1',
        partition_name    => 'p0',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => undef,
        partition_expression          => '',
        partition_method              => 'KEY',
        partition_name                => 'p0',
        partition_ordinal_position    => 1,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_get_partition(
    desc  => 'table k1 (KEY), partition p0',
    input => +{
        dbh               => $dbh_args,
        table             => 'k1',
        partition_name    => 'p1',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => undef,
        partition_expression          => '',
        partition_method              => 'KEY',
        partition_name                => 'p1',
        partition_ordinal_position    => 2,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_get_partition(
    desc  => 'table employees (HASH), partition p0',
    input => +{
        dbh               => $dbh_args,
        table             => 'employees',
        partition_name    => 'p0',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => undef,
        partition_expression          => 'YEAR(hired)',
        partition_method              => 'HASH',
        partition_name                => 'p0',
        partition_ordinal_position    => 1,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_get_partition(
    desc  => 'table employees (HASH), partition p1',
    input => +{
        dbh               => $dbh_args,
        table             => 'employees',
        partition_name    => 'p1',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => undef,
        partition_expression          => 'YEAR(hired)',
        partition_method              => 'HASH',
        partition_name                => 'p1',
        partition_ordinal_position    => 2,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_get_partition(
    desc  => 'table employees (HASH), partition p2',
    input => +{
        dbh               => $dbh_args,
        table             => 'employees',
        partition_name    => 'p2',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => undef,
        partition_expression          => 'YEAR(hired)',
        partition_method              => 'HASH',
        partition_name                => 'p2',
        partition_ordinal_position    => 3,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_get_partition(
    desc  => 'table employees (HASH), partition p3',
    input => +{
        dbh               => $dbh_args,
        table             => 'employees',
        partition_name    => 'p3',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => undef,
        partition_expression          => 'YEAR(hired)',
        partition_method              => 'HASH',
        partition_name                => 'p3',
        partition_ordinal_position    => 4,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_get_partition(
    desc  => 'table employees2 (LIST), partition pNorth',
    input => +{
        dbh               => $dbh_args,
        table             => 'employees2',
        partition_name    => 'pNorth',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => 'north',
        partition_description         => '3,5,6,9,17',
        partition_expression          => 'store_id',
        partition_method              => 'LIST',
        partition_name                => 'pNorth',
        partition_ordinal_position    => 1,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,

    },
);

test_get_partition(
    desc  => 'table employees2 (LIST), partition pEast',
    input => +{
        dbh               => $dbh_args,
        table             => 'employees2',
        partition_name    => 'pEast',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => 'east',
        partition_description         => '1,2,10,11,19,20',
        partition_expression          => 'store_id',
        partition_method              => 'LIST',
        partition_name                => 'pEast',
        partition_ordinal_position    => 2,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_get_partition(
    desc  => 'table employees2 (LIST), partition pWest',
    input => +{
        dbh               => $dbh_args,
        table             => 'employees2',
        partition_name    => 'pWest',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => 'west',
        partition_description         => '4,12,13,14,18',
        partition_expression          => 'store_id',
        partition_method              => 'LIST',
        partition_name                => 'pWest',
        partition_ordinal_position    => 3,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_get_partition(
    desc  => 'table employees2 (LIST), partition pCentral',
    input => +{
        dbh               => $dbh_args,
        table             => 'employees2',
        partition_name    => 'pCentral',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => 'central',
        partition_description         => '7,8,15,16',
        partition_expression          => 'store_id',
        partition_method              => 'LIST',
        partition_name                => 'pCentral',
        partition_ordinal_position    => 4,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_get_partition(
    desc  => 'table employees3 (RANGE), partition p0',
    input => +{
        dbh               => $dbh_args,
        table             => 'employees3',
        partition_name    => 'p0',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => '6',
        partition_expression          => 'store_id',
        partition_method              => 'RANGE',
        partition_name                => 'p0',
        partition_ordinal_position    => 1,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_get_partition(
    desc  => 'table employees3 (RANGE), partition p1',
    input => +{
        dbh               => $dbh_args,
        table             => 'employees3',
        partition_name    => 'p1',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => '11',
        partition_expression          => 'store_id',
        partition_method              => 'RANGE',
        partition_name                => 'p1',
        partition_ordinal_position    => 2,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_get_partition(
    desc  => 'table employees3 (RANGE), partition p2',
    input => +{
        dbh               => $dbh_args,
        table             => 'employees3',
        partition_name    => 'p2',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => '16',
        partition_expression          => 'store_id',
        partition_method              => 'RANGE',
        partition_name                => 'p2',
        partition_ordinal_position    => 3,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_get_partition(
    desc  => 'table employees3 (RANGE), partition p3',
    input => +{
        dbh               => $dbh_args,
        table             => 'employees3',
        partition_name    => 'p3',
        subpartition_name => undef,
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => '21',
        partition_expression          => 'store_id',
        partition_method              => 'RANGE',
        partition_name                => 'p3',
        partition_ordinal_position    => 4,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

undef $mysqld;

done_testing;

# Local Variables:
# mode: perl
# perl-indent-level: 4
# indent-tabs-mode: nil
# coding: utf-8-unix
# End:
#
# vim: expandtab shiftwidth=4:
