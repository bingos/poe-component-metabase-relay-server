package POE::Component::Metabase::Relay::Server::Queue;

use strict;
use warnings;
use POE qw[Component::EasyDBI];
use POE::Component::Client::HTTP;
use POE::Component::Metabase::Client::Submit;
use CPAN::Testers::Report     ();
use Metabase::User::Profile   ();
use Metabase::User::Secret    ();
use JSON ();
use Params::Util qw[_HASH];
use Time::HiRes ();
use Data::UUID;
use vars qw[$VERSION];

use constant DELAY => 150;

$VERSION = '0.10';

my $sql = {
  'create' => 'CREATE TABLE IF NOT EXISTS queue ( id varchar(150), submitted varchar(32), attempts INTEGER, data BLOB )',
  'insert' => 'INSERT INTO queue values(?,?,?,?)',
  'delete' => 'DELETE FROM queue where id = ?',
  'queue'  => 'SELECT * FROM queue ORDER BY attempts ASC, submitted ASC limit ', # the limit is appended via "submissions"
  'update' => 'UPDATE queue SET attempts = ? WHERE id = ?',
  'addidx' => [
	'CREATE INDEX IF NOT EXISTS queue_id ON queue ( id )',
	'CREATE INDEX IF NOT EXISTS queue_att_sub ON queue ( attempts, submitted )',
  ],
};

use MooseX::POE;
use MooseX::Types::URI qw[Uri];

has 'profile' => (
  is => 'ro',
  isa => 'Metabase::User::Profile',
  required => 1,
);
 
has 'secret' => (
  is => 'ro',
  isa => 'Metabase::User::Secret',
  required => 1,
);

has 'dsn' => (
  is => 'ro',
  isa => 'Str',
  required => 1,
);

has 'uri' => (
  is => 'ro',
  isa => Uri,
  coerce => 1,
  required => 1,
);

has 'username' => (
  is => 'ro',
  isa => 'Str',
  default => '',
);

has 'password' => (
  is => 'ro',
  isa => 'Str',
  default => '',
);

has 'debug' => (
  is => 'rw',
  isa => 'Bool',
  default => 0,
);

has 'multiple' => (
  is => 'ro',
  isa => 'Bool',
  default => 0,
);

has 'db_opts' => (
  is => 'ro',
  isa => 'HashRef',
  default => sub {{}},
);

has 'no_relay' => (
  is => 'rw',
  isa => 'Bool',
  default => 0,
  trigger => sub {
    my( $self, $new, $old ) = @_;
    return if ! $self->_has_easydbi;
    $self->yield( '_process_queue' ) if ! $new;
  },
);

has 'submissions' => (
  is => 'rw',
  isa => 'Int',
  default => 10,
);

has '_uuid' => (
  is => 'ro',
  isa => 'Data::UUID',
  lazy_build => 1,
  init_arg => undef,
);

has '_easydbi' => (
  is => 'ro',
  isa => 'POE::Component::EasyDBI',
  lazy_build => 1,
  init_arg => undef,
);

has _http_alias => (
  is => 'ro',
  isa => 'Str',
  init_arg => undef,
  writer => '_set_http_alias',
);

has '_processing' => (
  is => 'ro',
  isa => 'HashRef',
  default => sub {{}},
);

sub _build__easydbi {
  my $self = shift;
  POE::Component::EasyDBI->new(
    alias    => '',
    dsn      => $self->dsn,
    username => $self->username,
    password => $self->password,
    ( _HASH( $self->db_opts ) ? ( options => $self->db_opts ) : () ),
  );
}

sub _build__uuid {
  Data::UUID->new();
}

sub spawn {
  shift->new(@_);
}
 
sub START {
  my ($kernel,$self) = @_[KERNEL,OBJECT];
  $self->_build_table;
  $kernel->yield( 'do_vacuum' );
  if ( ! $self->multiple ) {
    $self->_set_http_alias( join '-', __PACKAGE__, $self->get_session_id );
    POE::Component::Client::HTTP->spawn(
      Alias           => $self->_http_alias,
      FollowRedirects => 2,
    );
  }
  $kernel->yield( '_process_queue' ) if ! $self->no_relay;
  return;
}

sub _build_table {
  my $self = shift;

  $self->_easydbi;

  if ( $self->dsn =~ /^dbi\:SQLite/i ) {
    $self->_easydbi->do(
      sql => 'PRAGMA synchronous = OFF',
      event => '_generic_db_result',
      _ts => $self->_time,
    );
  }

  $self->_easydbi->do(
    sql => $sql->{create},
    event => '_generic_db_result',
    _ts => $self->_time,
  );

  foreach my $idx ( @{ $sql->{addidx} } ) {
    $self->_easydbi->do(
      sql => $idx,
      event => '_generic_db_result',
      _ts => $self->_time,
    );
  }
}

event 'do_vacuum' => sub {
  my ($kernel,$self) = @_[KERNEL,OBJECT];
  $self->_easydbi->do(
    sql => 'VACUUM',
    event => '_generic_db_result',
    _ts => $self->_time,
  );

  $kernel->delay( 'do_vacuum' => DELAY * 60 );
  return;
};

event 'shutdown' => sub {
  my ($kernel,$self) = @_[KERNEL,OBJECT];
  $kernel->alarm_remove_all();
  $self->_easydbi->shutdown;
  $kernel->post( 
    $self->_http_alias,
    'shutdown',
  );
  return;
};

event '_generic_db_result' => sub {
  my ($kernel,$self,$result) = @_[KERNEL,OBJECT,ARG0];
  if ( $result->{error} ) {
    warn "DB error (" . ( $self->_time - $result->{_ts} ) . "s): " . JSON->new->pretty(1)->encode( $result ) . "\n" if $self->debug;
  }
  $kernel->yield( '_process_queue' ) if $result->{_process};
  return;
};

event 'submit' => sub {
  my ($kernel,$self,$fact) = @_[KERNEL,OBJECT,ARG0];
  return unless $fact and $fact->isa('Metabase::Fact');
  my $timestamp = $self->_time;
  $self->_easydbi->do(
    sql => $sql->{insert},
    event => '_generic_db_result',
    placeholders => [ $self->_uuid->create_b64(), $timestamp, 0, $self->_encode_fact( $fact ) ],
    ( $self->no_relay ? () : ( _process => 1 ) ),
    _ts => $timestamp,
  );
  return;
};

event '_process_queue' => sub {
  my ($kernel,$self) = @_[KERNEL,OBJECT];
  return if $self->no_relay;
  $kernel->delay( '_process_queue', DELAY );
  $self->_easydbi->arrayhash(
    sql => $sql->{queue} . ( $self->multiple ? $self->submissions : 1 ),
    event => '_queue_db_result',
    _ts => $self->_time,
  );
  return;
};

event '_queue_db_result' => sub {
  my ($kernel,$self,$result) = @_[KERNEL,OBJECT,ARG0];
  if ( $result->{error} and $self->debug ) {
    warn $result->{error}, "\n";
    return;
  }
# warn "Queue SQL took " . ( $self->_time - $result->{_ts} ) . "s to process\n";
  foreach my $row ( @{ $result->{result} } ) {
    # Have we seen this report before?
    if ( exists $self->_processing->{ $row->{id} } ) {
#      warn "Queue retrieved same fact '$row->{id}', skipping\n" if $self->debug;
      next;
    } else {
      $self->_processing->{ $row->{id} }++;
    }

    my $report = $self->_decode_fact( $row->{data} );
#    warn "Queue retrieved '$row->{id}' for processing\n" if $self->debug;
    POE::Component::Metabase::Client::Submit->submit(
      event   => '_submit_status',
      profile => $self->profile,
      secret  => $self->secret,
      fact    => $report,
      uri     => $self->uri->as_string,
      context => [ $row->{id}, $row->{attempts}, $self->_time ],
      ( $self->multiple ? () : ( http_alias => $self->_http_alias ) ),
    );
    
  }
  return;
};

event '_clear_processing' => sub {
  my($kernel,$self,$id) = @_[KERNEL,OBJECT,ARG0];
  delete $self->_processing->{ $id } if exists
    $self->_processing->{ $id };

  return;
};

event '_submit_status' => sub {
  my ($kernel,$self,$res) = @_[KERNEL,OBJECT,ARG0];
  my ($id,$attempts,$starttime) = @{ $res->{context} };
  my $timestamp = $self->_time;
  $kernel->delay_set( '_clear_processing' => DELAY, $id );
  if ( $res->{success} ) {
    warn "Submit '$id' (" . ( $timestamp - $starttime ) . "s) success\n" if $self->debug;
    $self->_easydbi->do(
      sql => $sql->{delete},
      event => '_generic_db_result',
      placeholders => [ $id ],
      ( $self->no_relay ? () : ( _process => 1 ) ),
      _ts => $timestamp,
    );
  }
  else {
    warn "Submit '$id' (" . ( $timestamp - $starttime ) . "s) error: $res->{error}\n" . ( defined $res->{content} ? "$res->{content}\n" : '' ) if $self->debug;
    if ( defined $res->{content} and $res->{content} =~ /GUID conflicts with an existing object/i ) {
      $self->_easydbi->do(
        sql => $sql->{delete},
        event => '_generic_db_result',
        placeholders => [ $id ],
        _ts => $timestamp,
      );
    }
    else {
      $attempts++;
      $self->_easydbi->do(
        sql => $sql->{update},
        event => '_generic_db_result',
        placeholders => [ $attempts, $id ],
        _ts => $timestamp,
      );
    }
  }
  return;
};

sub _time {
  return Time::HiRes::time;
}

sub _encode_fact {
  my $self = shift;
  return JSON->new->encode( shift->as_struct );
}

sub _decode_fact {
  my $self = shift;
  return CPAN::Testers::Report->from_struct( JSON->new->decode( shift ) );
}

no MooseX::POE;
 
__PACKAGE__->meta->make_immutable;
 
1;

__END__

=head1 NAME

POE::Component::Metabase::Relay::Server::Queue - Submission queue for the metabase relay

=head1 DESCRIPTION

POE::Component::Metabase::Relay::Server::Queue is the submission queue for L<POE::Component::Metabase::Relay::Server>.

It is based on L<POE::Component::EasyDBI> database and uses L<POE::Component::Metabase::Client::Submit> to send
reports to a L<Metabase> server.

=head1 CONSTRUCTOR

=over

=item C<spawn>

Spawns a new component session and creates a SQLite database if it doesn't already exist.

Takes a number of mandatory parameters:

  'dsn', a DBI DSN to use to store the submission queue;
  'profile', a Metabase::User::Profile object;
  'secret', a Metabase::User::Secret object;
  'uri', the uri of metabase server to submit to;

and a number of optional parameters:

  'username', a DSN username if required;
  'password', a DSN password if required;
  'db_opts', a hashref of DBD options that is passed to POE::Component::EasyDBI;
  'debug', enable debugging information;
  'multiple', set to true to enable the Queue to use multiple PoCo-Client-HTTPs, default 0;
  'no_relay', set to true to disable report submissions to the Metabase, default 0;
  'submissions', an int to control the number of parallel http clients ( used only if multiple == 1 ), default 10;

=back

=head1 INPUT EVENTS

=over

=item C<submit>

Takes one parameter a L<Metabase::Fact> to submit.

=item C<shutdown>

Terminates the component.

=back

=head1 AUTHOR

Chris C<BinGOs> Williams

=head1 LICENSE

Copyright E<copy> Chris Williams

This module may be used, modified, and distributed under the same terms as Perl itself. Please see the license that came with your Perl distribution for details.

=head1 SEE ALSO

L<Metabase>

L<Metabase::User::Profile>

L<Metabase::User::Secret>

L<POE::Component::Metabase::Client::Submit>

L<POE::Component::Metabase::Relay::Server>

L<POE::Component::EasyDBI>

=cut
