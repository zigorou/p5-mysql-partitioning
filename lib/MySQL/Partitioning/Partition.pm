package MySQL::Partitioning::Partition;

use strict;
use warnings;
use parent qw(Class::Accessor::Fast);
use Scalar::Util qw(isweak weaken);
use String::ShellQuote qw(shell_quote);

use Data::Dump qw(dump);

our $VERSION = '0.01';
my @COLUMNS = qw/PARTITION_NAME SUBPARTITION_NAME PARTITION_ORDINAL_POSITION
              SUBPARTITION_ORDINAL_POSITION PARTITION_METHOD SUBPARTITION_METHOD
              PARTITION_EXPRESSION SUBPARTITION_EXPRESSION PARTITION_DESCRIPTION
              PARTITION_COMMENT NODEGROUP TABLESPACE_NAME/;

__PACKAGE__->mk_accessors(
    qw/manager/,
    ( map { lc } @COLUMNS ),
);

sub new {
    my ( $class, %args ) = @_;

    unless ( isweak $args{manager} ) {
        weaken $args{manager};
    }
    
    $class->SUPER::new( \%args );
}

sub dbh { $_[0]->manager->dbh; }
sub sql { $_[0]->manager->sql; }
sub host { $_[0]->manager->host; }
sub database { $_[0]->manager->database; }
sub user { $_[0]->manager->user; }
sub credentical { $_[0]->manager->credentical; }
sub table { $_[0]->manager->table; }
sub mysqldump { $_[0]->manager->mysqldump; }

sub load {
    my $self = shift;

    my ( $stmt, @bind ) = $self->sql->select(
        'information_schema.PARTITIONS',
        [ @COLUMNS ],
        +{
            TABLE_SCHEMA => $self->database,
            TABLE_NAME   => $self->table,
            ( $self->partition_name )
            ? (
                PARTITION_NAME => $self->partition_name,
                ( defined $self->subpartition_name )
                ? ( SUBPARTITION_NAME => $self->subpartition_name )
                : (),
              )
            : (
                PARTITION_ORDINAL_POSITION => $self->partition_ordinal_position,
                ( defined $self->subpartition_ordinal_position )
                ? ( SUBPARTITION_ORDINAL_POSITION =>
                      $self->subpartition_ordinal_position )
                : ()
            ),
        },
    );
    my $rv = $self->dbh->selectrow_hashref( $stmt, undef, @bind );
    if ($rv) {
        for my $field ( keys %$rv ) {
            my $accessor = lc $field;
            $self->$accessor( $rv->{$field} );
        }
    }
}

sub mysqldump_command {
    my ( $self, @options ) = @_;

    unless ( @options > 0 ) {
        @options = (
            '--complete-insert', '--compress', '--compact',
            '--extended-insert', '--lock-tables', '--no-create-info',
        );
    }

    if ( defined $self->user && length $self->user > 0 ) {
        push( @options, '--user', $self->user );
    }

    if ( defined $self->credentical && length $self->credentical > 0 ) {
        push( @options, '--password', $self->credentical );
    }
    
    if ( defined $self->host && length $self->host > 0 ) {
        push( @options, '--host', $self->host );
    }

    my ( $stmt, @bind ) = $self->sql->_recurse_where($self->where);
    $stmt =~ s/\%{1}/\%\%/g;
    $stmt =~ s/\?/\%d/g;
    my $where = sprintf($stmt, @bind);

    push( @options, '--where', shell_quote($where) );
    push( @options, $self->database, $self->table );

    return join(' ' => $self->mysqldump, @options);
}

sub where {
    my ( $self, $where, $is_parent ) = @_;
    $where ||= +{};
    $is_parent = 1 unless defined $is_parent;

    my ( $method, $exp, $desc, $pos ) =
      ($is_parent)
      ? (
        $self->partition_method,      $self->partition_expression,
        $self->partition_description, $self->partition_ordinal_position,
      )
      : (
        $self->subpartition_method, $self->subpartition_expression,
        undef, $self->subpartition_ordinal_position
      );

    if ( $method eq 'RANGE' ) {
        ### TODO: considering subpartition
        unless ( $desc eq 'MAXVALUE' ) {
            $where->{$exp} = +{ '<' => $desc };
        }

        if ( my $previous_partition = $self->previous_partition ) {
            $where->{$exp}{'>='} = $previous_partition->partition_description;
        }
    }
    elsif ( $method eq 'LIST' ) {
        $where->{$exp} = +{ -in => [ split ',' => $desc ] };
    }
    elsif ( $method eq 'HASH' || $method eq 'KEY' ) {
        my $where_cause = [
            sprintf( '%% %d = ?', $self->partitions_length($is_parent) ),
            $pos - 1
        ];
        $exp ||= join( ', ', $self->primary_keys );
        $where->{$exp} = \$where_cause;
    }

    return $where;
}

sub previous_partition {
    my $self = shift;
    my ( $p_pos, $s_pos ) = (
        $self->partition_ordinal_position,
        $self->subpartition_ordinal_position
    );

    if ( defined $s_pos ) {
        if ( $s_pos > 1 ) {
            $s_pos--;
        }
        elsif ( $p_pos > 1 ) {
            $p_pos--;
        }
        else {
            return;
        }
    }
    else {
        if ( $p_pos > 1 ) {
            $p_pos--;
        }
        else {
            return;
        }
    }

    my $class             = ref $self;
    my $previous_position = $class->new(
        manager => $self,
        partition_ordinal_position    => $p_pos,
        subpartition_ordinal_position => $s_pos
    );
    $previous_position->load;
    return $previous_position;
}

sub partitions_length {
    my ( $self, $is_parent ) = @_;
    $is_parent = 1 unless defined $is_parent;

    my $pos_column =
      $is_parent
      ? 'PARTITION_ORDINAL_POSITION'
      : 'SUBPARTITION_ORDINAL_POSITION';

    my ( $stmt, @bind ) = $self->sql->select(
        'information_schema.PARTITIONS',
        [$pos_column],
        +{
            TABLE_SCHEMA => $self->database,
            TABLE_NAME   => $self->table,
            $is_parent
            ? ()
            : ( PARTITION_ORDINAL_POSITION =>
                  $self->partition_ordinal_position )
        },
        [ +{ -desc => [$pos_column] } ],
        1, 0
    );

    my ($partitions_length) =
      $self->dbh->selectrow_array( $stmt, undef, @bind );

    return $partitions_length;
}

sub primary_keys {
    my $self = shift;

    my $sth =
      $self->dbh->primary_key_info( undef, $self->database, $self->table );
    $sth->execute;
    my $rs = $sth->fetchall_arrayref( +{} );

    map { $_->{COLUMN_NAME} } sort { $a->{KEY_SEQ} <=> $b->{KEY_SEQ} } @$rs;
}

sub values {
    my $self = shift;
    if ( $self->partition_method eq 'LIST' ) {
        return [ split ',' => $self->partition_description ];
    }
    else {
        return $self->partition_description;
    }
}

1;

__END__

=head1 NAME

MySQL::Partitioning::Partition - write short description for MySQL::Partitioning::Partition

=head1 SYNOPSIS

  use MySQL::Partitioning::Partition;

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
