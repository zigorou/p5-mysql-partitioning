package MySQL::Partitioning;

use strict;
use warnings;
use parent qw(Class::Accessor::Fast);
use DBI qw(:sql_types);
use Data::Util qw(is_array_ref is_hash_ref);
use File::Which qw(which);
use Scalar::Util qw(weaken);
use SQL::Abstract::Limit;

use MySQL::Partitioning::Partition;

use Data::Dump qw(dump);

our $VERSION = '0.01';
my @COLUMNS = qw/PARTITION_NAME SUBPARTITION_NAME PARTITION_ORDINAL_POSITION
  SUBPARTITION_ORDINAL_POSITION PARTITION_METHOD SUBPARTITION_METHOD
  PARTITION_EXPRESSION SUBPARTITION_EXPRESSION PARTITION_DESCRIPTION
  PARTITION_COMMENT NODEGROUP TABLESPACE_NAME/;

__PACKAGE__->mk_accessors(
    qw/dbh sql host database user credentical table mysqldump partitions_map/);

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

    $args{partitions} = [];
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
            +{ -asc =>
              [qw/PARTITION_ORDINAL_POSITION SUBPARTITION_ORDINAL_POSITION/] }
        ],
    );

    my $rs = $self->dbh->selectall_arrayref( $stmt, +{ Slice => +{} }, @bind );

    my %partitions_map;
    my $manager = $self;
    weaken $manager;

    for my $rv (@$rs) {
        my %new_args = (
            manager => $manager,
            ( map { lc $_ => $rv->{$_} } keys %$rv ),
        );

	my $partition = MySQL::Partitioning::Partition->new(%new_args);

	if ( defined $partition->subpartition_name ) {
	    $partitions_map{ $partition->partition_name } ||= +{};
	    $partitions_map{ $partition->partition_name }->{ $partition->subpartition_name } ||= $partition;
	}
	else {
	    $partitions_map{ $partition->partition_name } ||= $partition;
	}
    }

    $self->partitions_map( \%partitions_map );
}

sub get_partition {
    my ( $self, $partition_name, $subpartition_name) = @_;

    unless ( exists $self->partitions_map->{$partition_name} ) {
	my $partition = MySQL::Partitioning::Partition->new(
	    manager => $self,
	    partition_name => $partition_name,
	    defined $subpartition_name ? ( subpartition_name => $subpartition_name ) : (),
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
