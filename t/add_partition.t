use strict;
use warnings;
use lib 't/lib';

use Test::More;
use Test::Exception;
use Test::MySQL::Partitioning;

use MySQL::Partitioning;

my $mysqld = Test::MySQL::Partitioning->setup(0);
my $dbh_args = [
    $mysqld->dsn( dbname => 'test_partitioning' ),
    'root', '', +{ AutoCommit => 1, RaiseError => 1 }
];

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

sub test_add_partition {
    my %specs = @_;
    my ( $input, $expects, $desc ) = @specs{qw/input expects desc/};
    my ( $dbh, $table, $args ) = @$input{qw/dbh table args/};
    subtest $desc => sub {
        my $p = MySQL::Partitioning->new(
            dbh => $dbh,
            user => 'root',
            credentical => '',
            table => $table,
        );

        Test::MySQL::Partitioning->setup_tables( $p->dbh );

        lives_ok { $p->add_partition( %$args ); } 'add_partition() lives ok';
        test_partition( $p->get_partition( $args->{partition_name} ), $expects );

        my ( $stmt, @bind ) = $p->_add_partition_sql( %$args );
        note $stmt;
        note explain \@bind;
        
        Test::MySQL::Partitioning->teardown_tables( $p->dbh );
    };
}

test_add_partition(
    desc => 'table: employee2, defined: values',
    input => +{
        table => 'employees2',
        dbh => $dbh_args,
        args => +{
            partition_name => 'pTest',
            values => [ 100, 200, 300 ],
        },
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => '100,200,300',
        partition_expression          => 'store_id',
        partition_method              => 'LIST',
        partition_name                => 'pTest',
        partition_ordinal_position    => 5,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_add_partition(
    desc => 'table: employee2, defined: values, engine, comment',
    input => +{
        table => 'employees2',
        dbh => $dbh_args,
        args => +{
            partition_name => 'pTest',
            values => [ 100, 200, 300 ],
            engine => 'InnoDB',
            comment => 'test',
        },
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => 'test',
        partition_description         => '100,200,300',
        partition_expression          => 'store_id',
        partition_method              => 'LIST',
        partition_name                => 'pTest',
        partition_ordinal_position    => 5,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_add_partition(
    desc => 'table: employees3, defined: values',
    input => +{
        table => 'employees3',
        dbh => $dbh_args,
        args => +{
            partition_name => 'pTest',
            values => 30,
        },
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => '30',
        partition_expression          => 'store_id',
        partition_method              => 'RANGE',
        partition_name                => 'pTest',
        partition_ordinal_position    => 5,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_add_partition(
    desc => 'table: employees3, defined: values, engine, comment',
    input => +{
        table => 'employees3',
        dbh => $dbh_args,
        args => +{
            partition_name => 'pTest',
            values => 30,
            engine => 'InnoDB',
            comment => 'pTest',
        },
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => 'pTest',
        partition_description         => '30',
        partition_expression          => 'store_id',
        partition_method              => 'RANGE',
        partition_name                => 'pTest',
        partition_ordinal_position    => 5,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_add_partition(
    desc => 'table: employees3, defined: values equals MAXVALUE',
    input => +{
        table => 'employees3',
        dbh => $dbh_args,
        args => +{
            partition_name => 'pTest',
            values => \'MAXVALUE',
        },
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '',
        partition_description         => 'MAXVALUE',
        partition_expression          => 'store_id',
        partition_method              => 'RANGE',
        partition_name                => 'pTest',
        partition_ordinal_position    => 5,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
);

test_add_partition(
    desc => 'table: activities, defined: values using function',
    input => +{
        table => 'activities',
        dbh => $dbh_args,
        args => +{
            partition_name => 'p20100916',
            values => \[ 'TO_DAYS(?)', '2010-09-17 00:00:00' ],
            comment => '2010-09-16',
            engine => 'InnoDB',
        },
    },
    expects => +{
        nodegroup                     => 'default',
        partition_comment             => '2010-09-16',
        partition_description         => '734397', ### TO_DAYS('2010-09-17 00:00:00')
        partition_expression          => 'TO_DAYS(created_on)',
        partition_method              => 'RANGE',
        partition_name                => 'p20100916',
        partition_ordinal_position    => 3,
        subpartition_expression       => undef,
        subpartition_method           => undef,
        subpartition_name             => undef,
        subpartition_ordinal_position => undef,
        tablespace_name               => undef,
    },
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
