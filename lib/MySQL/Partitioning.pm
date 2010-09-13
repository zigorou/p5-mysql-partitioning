package MySQL::Partitioning;

use strict;
use warnings;
use parent qw(Class::Accessor::Fast);
use Data::Util qw(is_array_ref is_hash_ref is_scalar_ref is_number is_integer);
use DBI qw(:sql_types);
use Carp;
use File::Which qw(which);
use Scalar::Util qw(weaken);
use SQL::Abstract::Limit;
use Try::Tiny;

use MySQL::Partitioning::Partition;

our $VERSION = '0.01';
my @COLUMNS = qw/PARTITION_NAME SUBPARTITION_NAME PARTITION_ORDINAL_POSITION
  SUBPARTITION_ORDINAL_POSITION PARTITION_METHOD SUBPARTITION_METHOD
  PARTITION_EXPRESSION SUBPARTITION_EXPRESSION PARTITION_DESCRIPTION
  PARTITION_COMMENT NODEGROUP TABLESPACE_NAME/;

__PACKAGE__->mk_accessors(
    qw/dbh sql host database user credentical table mysqldump partitions partitions_map/
);

sub new {
    my ( $class, %args ) = @_;

    if ( exists $args{dbh} && is_array_ref( $args{dbh} ) ) {
        my ( $dsn, $user, $credentical, $attrs ) = @{ $args{dbh} };
        $attrs->{RaiseError} ||= 1;

        $args{dbh}         = DBI->connect( $dsn, $user, $credentical, $attrs );
        $args{user}        = $user;
        $args{credentical} = $credentical;
    }

    unless ( exists $args{mysqldump} ) {
        my @cmds = which 'mysqldump';
        $args{mysqldump} = shift @cmds;
    }

    $args{partitions_map} = +{};
    $args{partitions}     = [];
    $args{sql} ||= SQL::Abstract::Limit->new( limit_dialect => $args{dbh} );

    my $self = $class->SUPER::new( \%args );
    $self->_init_from_dbh;
    $self;
}

sub load {
    my $self = shift;

    my ( $stmt, @bind ) = $self->sql->select(
        'information_schema.PARTITIONS',
        [@COLUMNS],
        +{
            TABLE_SCHEMA => $self->database,
            TABLE_NAME   => $self->table,
        },
        [
            +{
                -asc => [
                    qw/PARTITION_ORDINAL_POSITION SUBPARTITION_ORDINAL_POSITION/
                ]
            }
        ],
    );

    my $rs = $self->dbh->selectall_arrayref( $stmt, +{ Slice => +{} }, @bind );

    my %partitions_map;
    my @partitions;
    my $manager = $self;
    weaken $manager;

    for my $rv (@$rs) {
        my %new_args = (
            manager => $manager,
            ( map { lc $_ => $rv->{$_} } keys %$rv ),
        );

        my $partition = MySQL::Partitioning::Partition->new(%new_args);
        push( @partitions, $partition );

        if ( defined $partition->subpartition_name ) {
            $partitions_map{ $partition->partition_name } ||= +{};
            $partitions_map{ $partition->partition_name }
              ->{ $partition->subpartition_name } ||= $partition;
        }
        else {
            $partitions_map{ $partition->partition_name } ||= $partition;
        }
    }

    $self->partitions( \@partitions );
    $self->partitions_map( \%partitions_map );
}

sub get_partition {
    my ( $self, $partition_name, $subpartition_name ) = @_;

    unless ( exists $self->partitions_map->{$partition_name} ) {
        my $partition = MySQL::Partitioning::Partition->new(
            manager        => $self,
            partition_name => $partition_name,
            defined $subpartition_name
            ? ( subpartition_name => $subpartition_name )
            : (),
        );
        $partition->load;
        return $partition;
    }
    else {
        my $p = $self->partitions_map->{$partition_name};

        if ( is_hash_ref $p ) {
            return $p->{$subpartition_name};
        }
        else {
            return $p;
        }
    }
}

sub add_partition {
    my ( $self, %args ) = @_;
    my $dbh = $self->dbh;
    my ( $stmt, @bind ) = $self->_add_partition_sql(%args);

    try {
        my $sth = $dbh->prepare($stmt) or croak( $dbh->errstr );
        my $i = 1;
        for (@bind) {
            $sth->bind_param( $i++, @$_ ) or croak( $sth->errstr );
        }

        $sth->execute or croak( $sth->errstr );
    }
    catch {
        my $e = $_;
        croak $e;
    };

    return 1;
}

sub drop_partition {
    my ( $self, $partition_name ) = @_;
    my $dbh = $self->dbh;
    my $stmt =
        'ALTER TABLE '
      . $dbh->quote_identifier( $self->table )
      . ' DROP PARTITION '
      . $partition_name;

    try {
        $dbh->do($stmt);
    }
    catch {
        my $e = $_;
        croak $e;
    };

    return 1;
}

sub _add_partition_sql {
    my ( $self, %args ) = @_;
    my $dbh = $self->dbh;
    my ( $stmt, @bind );

    $stmt =
        'ALTER TABLE '
      . $dbh->quote_identifier( $self->table )
      . ' ADD PARTITION ( PARTITION '
      . $args{partition_name} . ' ';

    my $values = $args{values};

    if ( is_array_ref $values ) {
        ### [ 1, 3, 5 ]
        $stmt .=
          sprintf( 'VALUES IN (%s) ', substr( '?, ' x @{$values}, 0, -2 ) );
        push( @bind, map { [ $_, SQL_INTEGER ] } @{$values} );
    }
    elsif ( is_scalar_ref($values) ) {
        my $values_ref = $$values;
        if ( is_array_ref $values_ref ) {
	    my @values = @$values_ref;
            ### \[ TO_DAYS(?), '2010-05-08 00:00:00' ]
            $stmt .= sprintf( 'VALUES LESS THAN (%s) ', shift @values );
            push(
                @bind,
                map {
                    [
                        $_,
                        is_integer($_)
                        ? SQL_INTEGER
                        : ( is_number($_) ? SQL_FLOAT : SQL_VARCHAR )
                    ]
                  } @values
            );
        }
        else {
            ### \'MAXVALUE'
            $stmt .= sprintf( 'VALUES LESS THAN %s ', $values_ref );
        }
    }
    else {
        $stmt .= 'VALUES LESS THAN (?) ';
        push(
            @bind,
            [
                $values,
                is_number($values)
                ? ( is_integer($values) ? SQL_INTEGER : SQL_FLOAT )
                : SQL_VARCHAR
            ]
        );
    }

    $self->_add_statement( \$stmt, 'ENGINE', $args{engine} );
    $self->_add_statement_and_bind( \$stmt, \@bind, 'COMMENT', $args{comment},
        SQL_VARCHAR );
    $self->_add_statement_and_bind( \$stmt, \@bind, 'DATA DIRECTORY',
        $args{data_directory}, SQL_VARCHAR );
    $self->_add_statement_and_bind( \$stmt, \@bind, 'INDEX DIRECTORY',
        $args{index_directory}, SQL_VARCHAR );
    $self->_add_statement_and_bind( \$stmt, \@bind, 'MAX_ROWS', $args{max_rows},
        SQL_INTEGER );
    $self->_add_statement_and_bind( \$stmt, \@bind, 'MIN_ROWS', $args{min_rows},
        SQL_INTEGER );
    $self->_add_statement( \$stmt, 'TABLESPACE',
        '( ' . $args{tablespace_name} . ' )' )
      if ( defined $args{tablespace_name} );
    $self->_add_statement( \$stmt, 'NODEGROUP', $args{node_group_id} );

    if ( is_array_ref $args{subpartitions} ) {
        ### TODO: supports subpartitions
        for ( @{ $args{subpartitions} } ) {
        }
    }

    $stmt .= ')';

    return ( $stmt, @bind );
}

sub _add_statement {
    my ( $self, $stmt_ref, $attr, $value ) = @_;
    if ( defined $value && length $value > 0 ) {
        $$stmt_ref .= sprintf( '%s = %s ', $attr, $value );
    }
}

sub _add_statement_and_bind {
    my ( $self, $stmt_ref, $bind_ref, $attr, $value, $sql_type ) = @_;
    if ( defined $value && length $value > 0 ) {
        $$stmt_ref .= sprintf( '%s = ? ', $attr );
        push( @$bind_ref, [ $value, $sql_type ] );
    }
}

sub _init_from_dbh {
    my $self = shift;
    my %dsn = map { split '=' => $_ } split( ';' => $self->dbh->{Name} );

    unless ( defined $self->database ) {
        $self->database( $dsn{dbname} || $dsn{db} );
    }

    unless ( defined $self->host ) {
        $self->host( $dsn{host} ) if ( exists $dsn{host} );
    }
}

1;
__END__

=head1 NAME

MySQL::Partitioning -

=head1 SYNOPSIS

  use MySQL::Partitioning;

=head1 DESCRIPTION

MySQL::Partitioning is

=head1 AUTHOR

Toru Yamaguchi E<lt>zigorou@cpan.orgE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
