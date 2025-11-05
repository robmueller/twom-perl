package Twom;

use strict;
use warnings;
require XSLoader;
our $VERSION = '0.01';

# Export return codes only (flags are now passed as hashrefs)
use Exporter 'import';
our @EXPORT_OK = qw(
  TWOM_OK TWOM_DONE TWOM_EXISTS TWOM_IOERROR TWOM_INTERNAL TWOM_LOCKED
  TWOM_NOTFOUND TWOM_READONLY TWOM_BADFORMAT TWOM_BADUSAGE TWOM_BADCHECKSUM
);

XSLoader::load(__PACKAGE__, $VERSION);

1;

package Twom::DB;    # Opaque handle
use strict; use warnings; 1;

package Twom::Txn;   # Opaque handle
use strict; use warnings; 1;

package Twom::Cursor;# Opaque handle
use strict; use warnings; 1;

__END__

=head1 NAME

Twom - Perl interface to the Twom MVCC key-value store

=head1 SYNOPSIS

  use Twom;

  # Open or create a database
  my $db = Twom->open("mydb.twom", { create => 1 });

  # Simple key-value operations
  $db->store("key", "value");
  my $value = $db->fetch("key");

  # Store with flags
  $db->store("unique_key", "data", { ifnotexist => 1 });
  $db->store("existing_key", "updated", { ifexist => 1 });

  # Iterate over keys with a prefix
  $db->foreach("user:", sub {
      my ($context, $key, $value) = @_;
      print "$key => $value\n";
      return 0; # continue iteration
  });

  # Use transactions for atomic operations
  my $txn = $db->begin_txn(0);  # 0 = write transaction
  $txn->store("counter", "1");
  $txn->store("timestamp", time());
  $txn->commit();

  # Use cursors for iteration
  my $cur = $db->begin_cursor("prefix:");
  while (my ($key, $val) = $cur->next) {
      print "$key => $val\n";
  }
  $cur->commit();

=head1 DESCRIPTION

Twom is a Perl interface to the twom C library, which provides a single-file,
MVCC (Multi-Version Concurrency Control) key-value store. This module offers
a Perl-friendly API with hashref-based flags instead of integer constants.

The database supports:

=over 4

=item * ACID transactions with MVCC semantics

=item * Efficient prefix-based iteration

=item * Multiple checksum algorithms

=item * Shared (read-only) and exclusive (read-write) access modes

=item * Cursor-based traversal

=item * Atomic conditional operations (insert-if-not-exists, update-if-exists)

=back

=head1 METHODS

=head2 Twom Class Methods

=head3 open

  my $db = Twom->open($filename);
  my $db = Twom->open($filename, \%flags);

Opens or creates a Twom database file. Returns a C<Twom::DB> object.

B<Parameters:>

=over 4

=item * C<$filename> - Path to the database file

=item * C<\%flags> - Optional hashref of flags

=back

B<Flags:>

=over 4

=item * C<create> - Create the database if it doesn't exist

=item * C<shared> - Open in shared (read-only) mode

=item * C<nocsum> - Disable checksums entirely

=item * C<nosync> - Don't sync to disk on commits (faster but less safe)

=item * C<nonblocking> - Use non-blocking locks

=item * C<csum_null> - Use NULL checksum algorithm

=item * C<csum_xxh64> - Use XXH64 checksum algorithm

=back

B<Examples:>

  # Create a new database
  my $db = Twom->open("data.twom", { create => 1 });

  # Open existing database in shared mode
  my $db = Twom->open("data.twom", { shared => 1 });

  # Create with specific checksum
  my $db = Twom->open("data.twom", {
      create => 1,
      csum_xxh64 => 1
  });

=head3 strerror

  my $msg = Twom->strerror($error_code);

Returns a human-readable error message for a Twom error code.

B<Parameters:>

=over 4

=item * C<$error_code> - A Twom error code (negative integer)

=back

=head2 Twom::DB Methods

=head3 fname

  my $path = $db->fname();

Returns the filesystem path of the database file.

=head3 uuid

  my $uuid = $db->uuid();

Returns the database's UUID (unique identifier).

=head3 generation

  my $gen = $db->generation();

Returns the current generation number of the database. This number increments
after operations like repack.

=head3 num_records

  my $count = $db->num_records();

Returns the number of records currently in the database.

=head3 size

  my $bytes = $db->size();

Returns the size of the database file in bytes.

=head3 sync

  my $rc = $db->sync();

Explicitly syncs the database to disk. Returns 0 on success.

=head3 check_consistency

  my $rc = $db->check_consistency();

Checks the internal consistency of the database. Returns 0 if consistent.

=head3 repack

  my $rc = $db->repack();

Repacks the database file, removing dead space and incrementing the generation.
Returns 0 on success.

=head3 should_repack

  if ($db->should_repack()) {
      $db->repack();
  }

Returns true if the database would benefit from repacking.

=head3 yield

  $db->yield();

Yields the database lock momentarily to allow other processes access.
Useful in long-running operations.

=head3 fetch

  my $value = $db->fetch($key);
  my $value = $db->fetch($key, \%flags);

Retrieves the value associated with a key. Returns C<undef> if not found.

B<Parameters:>

=over 4

=item * C<$key> - The key to fetch

=item * C<\%flags> - Optional hashref of flags

=back

B<Flags:>

=over 4

=item * C<fetchnext> - Fetch the next key after the specified key

=back

B<Examples:>

  my $value = $db->fetch("username");

  # Fetch next key after "user:100"
  my $next = $db->fetch("user:100", { fetchnext => 1 });

=head3 store

  my $rc = $db->store($key, $value);
  my $rc = $db->store($key, $value, \%flags);

Stores a key-value pair. Returns 0 on success, or an error code on failure.

B<Parameters:>

=over 4

=item * C<$key> - The key to store

=item * C<$value> - The value to store

=item * C<\%flags> - Optional hashref of flags

=back

B<Flags:>

=over 4

=item * C<ifnotexist> - Only store if key doesn't exist (insert)

=item * C<ifexist> - Only store if key exists (update)

=item * C<nosync> - Don't sync to disk immediately

=back

B<Examples:>

  # Simple store
  $db->store("key", "value");

  # Atomic insert - fails if key exists
  eval {
      $db->store("unique_id", "data", { ifnotexist => 1 });
  };
  if ($@) {
      print "Key already exists\n";
  }

  # Atomic update - fails if key doesn't exist
  my $rc = $db->store("counter", "42", { ifexist => 1 });
  if ($rc != 0) {
      print "Key doesn't exist\n";
  }

=head3 dump

  $db->dump();
  $db->dump($detail_level);

Dumps database information to STDERR for debugging.

B<Parameters:>

=over 4

=item * C<$detail_level> - Optional detail level (0=basic, higher=more detail)

=back

=head3 foreach

  $db->foreach($prefix, \&callback);
  $db->foreach($prefix, \&callback, \%flags);
  $db->foreach($prefix, \&callback, \%flags, $context);

Iterates over keys with a given prefix, calling a callback for each.

B<Parameters:>

=over 4

=item * C<$prefix> - Key prefix to match (empty string matches all)

=item * C<\&callback> - Coderef called for each key: C<sub ($context, $key, $value) { ... }>

=item * C<\%flags> - Optional hashref of flags

=item * C<$context> - Optional context value passed to callback

=back

The callback should return 0 to continue iteration, or non-zero to stop.

B<Flags:>

=over 4

=item * C<alwaysyield> - Yield lock after each record

=item * C<noyield> - Never yield lock during iteration

=item * C<skiproot> - Skip the root node during iteration

=back

B<Examples:>

  # Print all keys with prefix "user:"
  $db->foreach("user:", sub {
      my ($ctx, $key, $value) = @_;
      print "$key => $value\n";
      return 0;
  });

  # Collect keys into array, stopping after 10
  my @keys;
  $db->foreach("", sub {
      my ($ctx, $key, $value) = @_;
      push @keys, $key;
      return scalar(@keys) >= 10 ? 1 : 0;
  });

  # Use context parameter
  my $total = 0;
  $db->foreach("counter:", sub {
      my ($sum_ref, $key, $value) = @_;
      $$sum_ref += $value;
      return 0;
  }, undef, \$total);

=head3 begin_txn

  my $txn = $db->begin_txn($shared);

Begins a new transaction. Returns a C<Twom::Txn> object.

B<Parameters:>

=over 4

=item * C<$shared> - 0 for write transaction, 1 for read-only shared transaction

=back

B<Examples:>

  # Write transaction
  my $txn = $db->begin_txn(0);
  $txn->store("key1", "value1");
  $txn->store("key2", "value2");
  $txn->commit();

  # Read-only transaction
  my $txn = $db->begin_txn(1);
  my $val = $txn->fetch("key");
  $txn->commit();

=head3 begin_cursor

  my $cur = $db->begin_cursor($start_key);
  my $cur = $db->begin_cursor($start_key, \%flags);

Creates a cursor for iterating over keys. Returns a C<Twom::Cursor> object.

B<Parameters:>

=over 4

=item * C<$start_key> - Key to start iteration from

=item * C<\%flags> - Optional hashref of flags

=back

B<Flags:>

=over 4

=item * C<mvcc> - Use MVCC semantics

=item * C<cursor_prefix> - Restrict iteration to keys with the start_key prefix

=item * C<noyield> - Don't yield lock during iteration

=back

B<Examples:>

  # Iterate from start
  my $cur = $db->begin_cursor("");
  while (my ($key, $val) = $cur->next) {
      print "$key => $val\n";
  }
  $cur->commit();

  # Iterate only keys with prefix
  my $cur = $db->begin_cursor("user:", { cursor_prefix => 1 });
  while (my ($key, $val) = $cur->next) {
      print "$key => $val\n";
  }
  $cur->commit();

=head2 Twom::Txn Methods

Transaction objects provide atomic, isolated operations on the database.
Always call C<commit()> to persist changes or C<abort()> to discard them.

=head3 commit

  my $rc = $txn->commit();

Commits the transaction, making all changes permanent. Returns 0 on success.
The transaction object becomes invalid after this call.

=head3 abort

  my $rc = $txn->abort();

Aborts the transaction, discarding all changes. Returns 0 on success.
The transaction object becomes invalid after this call.

=head3 yield

  $txn->yield();

Yields the transaction lock momentarily.

=head3 fetch

  my $value = $txn->fetch($key);
  my $value = $txn->fetch($key, \%flags);

Fetches a value within the transaction context. Same flags as C<Twom::DB::fetch>.

B<Flags:>

=over 4

=item * C<fetchnext> - Fetch the next key after the specified key

=back

=head3 store

  my $rc = $txn->store($key, $value);
  my $rc = $txn->store($key, $value, \%flags);

Stores a key-value pair within the transaction. Changes are not visible
outside the transaction until commit.

B<Flags:>

=over 4

=item * C<ifnotexist> - Only store if key doesn't exist

=item * C<ifexist> - Only store if key exists

=back

=head3 foreach

  $txn->foreach($prefix, \&callback);
  $txn->foreach($prefix, \&callback, \%flags);
  $txn->foreach($prefix, \&callback, \%flags, $context);

Iterates over keys within the transaction context. Same interface as
C<Twom::DB::foreach>.

B<Flags:>

=over 4

=item * C<alwaysyield> - Yield lock after each record

=item * C<noyield> - Never yield lock during iteration

=item * C<skiproot> - Skip the root node during iteration

=item * C<mvcc> - Use MVCC semantics

=back

=head3 begin_cursor

  my $cur = $txn->begin_cursor($start_key);
  my $cur = $txn->begin_cursor($start_key, \%flags);

Creates a cursor within the transaction context. Same interface as
C<Twom::DB::begin_cursor>.

B<Flags:>

=over 4

=item * C<mvcc> - Use MVCC semantics

=item * C<cursor_prefix> - Restrict to prefix

=item * C<noyield> - Don't yield lock

=back

=head2 Twom::Cursor Methods

Cursors provide efficient sequential access to key-value pairs.
Always call C<commit()> or C<abort()> when done.

=head3 commit

  my $rc = $cur->commit();

Commits the cursor, finalizing any changes made via C<replace()>.
The cursor object becomes invalid after this call.

=head3 abort

  my $rc = $cur->abort();

Aborts the cursor, discarding any changes. The cursor object becomes
invalid after this call.

=head3 next

  my ($key, $value) = $cur->next();

Advances to the next key-value pair and returns them. Returns an empty
list when no more records are available.

B<Examples:>

  my $cur = $db->begin_cursor("");
  while (my ($key, $val) = $cur->next) {
      print "$key => $val\n";
  }
  $cur->commit();

=head3 replace

  my $rc = $cur->replace($new_value);
  my $rc = $cur->replace($new_value, \%flags);

Replaces the value of the current record (the last one returned by C<next()>).

B<Parameters:>

=over 4

=item * C<$new_value> - The new value to store

=item * C<\%flags> - Optional hashref of flags

=back

B<Flags:>

=over 4

=item * C<ifnotexist> - Only replace if key doesn't exist

=item * C<ifexist> - Only replace if key exists

=back

=head1 RETURN CODES

The module exports the following return codes for error handling:

=over 4

=item * C<TWOM_OK> (0) - Success

=item * C<TWOM_DONE> (1) - Operation completed (e.g., iteration finished)

=item * C<TWOM_EXISTS> (-1) - Key already exists

=item * C<TWOM_IOERROR> (-2) - I/O error

=item * C<TWOM_INTERNAL> (-3) - Internal error

=item * C<TWOM_LOCKED> (-4) - Database is locked

=item * C<TWOM_NOTFOUND> (-5) - Key not found

=item * C<TWOM_READONLY> (-6) - Database is read-only

=item * C<TWOM_BADFORMAT> (-7) - Invalid database format

=item * C<TWOM_BADUSAGE> (-8) - Invalid API usage

=item * C<TWOM_BADCHECKSUM> (-9) - Checksum verification failed

=back

=head1 BEST PRACTICES

=head2 Error Handling

Most operations will croak on serious errors, but some return error codes:

  # store() returns error code for conditional operations
  my $rc = $db->store("key", "val", { ifexist => 1 });
  if ($rc != 0) {
      warn "Key doesn't exist: " . Twom->strerror($rc);
  }

  # Use eval to catch croaks
  eval {
      $db->store("key", "val", { ifnotexist => 1 });
  };
  if ($@) {
      warn "Failed to insert: $@";
  }

=head2 Transactions

Always commit or abort transactions:

  my $txn = $db->begin_txn(0);
  eval {
      $txn->store("key1", "val1");
      $txn->store("key2", "val2");
      $txn->commit();
  };
  if ($@) {
      $txn->abort();
      die "Transaction failed: $@";
  }

=head2 Cursors

Always commit or abort cursors:

  my $cur = $db->begin_cursor("");
  while (my ($key, $val) = $cur->next) {
      # process...
  }
  $cur->commit();

=head2 Resource Management

Database objects, transactions, and cursors are automatically cleaned up
when they go out of scope, but it's better to explicitly commit/abort:

  {
      my $txn = $db->begin_txn(0);
      # ... operations ...
      $txn->commit();
  } # txn is cleaned up here

=head1 EXAMPLES

=head2 Simple Cache

  my $db = Twom->open("cache.twom", { create => 1 });

  sub get_cached {
      my $key = shift;
      return $db->fetch($key);
  }

  sub set_cache {
      my ($key, $value) = @_;
      $db->store($key, $value);
  }

=head2 User Database

  my $db = Twom->open("users.twom", { create => 1 });

  # Add user (atomic - fails if exists)
  eval {
      $db->store("user:bob", encode_json(\%user_data),
                 { ifnotexist => 1 });
  };
  if ($@) {
      die "User already exists";
  }

  # Update user (atomic - fails if not exists)
  my $rc = $db->store("user:bob", encode_json(\%updated_data),
                      { ifexist => 1 });
  if ($rc != 0) {
      die "User not found";
  }

  # List all users
  $db->foreach("user:", sub {
      my ($ctx, $key, $json) = @_;
      my $user = decode_json($json);
      print "$key: $user->{name}\n";
      return 0;
  });

=head2 Atomic Counter

  my $db = Twom->open("counters.twom", { create => 1 });

  sub increment_counter {
      my $name = shift;
      my $txn = $db->begin_txn(0);

      eval {
          my $val = $txn->fetch($name) || 0;
          $txn->store($name, $val + 1);
          $txn->commit();
      };
      if ($@) {
          $txn->abort();
          die "Failed to increment: $@";
      }
  }

=head1 THREAD SAFETY

The underlying C library supports concurrent access from multiple processes
via file locking. However, individual Twom::DB objects should not be shared
between threads. Create separate database connections per thread if needed.

=head1 SEE ALSO

L<BerkeleyDB>, L<LMDB_File>, L<DBM::Deep>

=head1 AUTHOR

Twom Perl bindings by Rob Mueller

Twom C library by [original author]

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
