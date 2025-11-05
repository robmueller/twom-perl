use strict;
use warnings;

use Test::More;
use Twom qw(
    TWOM_OK TWOM_DONE TWOM_EXISTS TWOM_IOERROR TWOM_INTERNAL TWOM_LOCKED
    TWOM_NOTFOUND TWOM_READONLY TWOM_BADFORMAT TWOM_BADUSAGE TWOM_BADCHECKSUM
    TWOM_CREATE TWOM_SHARED TWOM_NOCSUM TWOM_NOSYNC TWOM_NONBLOCKING
    TWOM_ALWAYSYIELD TWOM_NOYIELD TWOM_IFNOTEXIST TWOM_IFEXIST TWOM_FETCHNEXT
    TWOM_SKIPROOT TWOM_MVCC TWOM_CURSOR_PREFIX
    TWOM_CSUM_NULL TWOM_CSUM_XXH64 TWOM_CSUM_EXTERNAL TWOM_COMPAR_EXTERNAL
);
use File::Temp;

# Verify the module loaded
ok(1, 'Twom module loaded');

# Test database open with various flags
{
    my $dbname = File::Temp->new;
    unlink $dbname;

    # Test CREATE flag
    my $db = Twom->open($dbname, { create => 1 });
    ok($db, "db created with create flag");
    $db = undef;

    # Test opening existing db without create flag
    $db = Twom->open($dbname);
    ok($db, "db opened without create flag");

    # Test NOCSUM flag
    my $dbname2 = File::Temp->new;
    unlink $dbname2;
    my $db2 = Twom->open($dbname2, { create => 1, nocsum => 1 });
    ok($db2, "db created with nocsum flag");
    $db2 = undef;

    # Test NOSYNC flag
    my $dbname3 = File::Temp->new;
    unlink $dbname3;
    my $db3 = Twom->open($dbname3, { create => 1, nosync => 1 });
    ok($db3, "db created with nosync flag");
    $db3 = undef;

    # Test MVCC flag
    my $dbname4 = File::Temp->new;
    unlink $dbname4;
    my $db4 = Twom->open($dbname4, { create => 1, mvcc => 1 });
    ok($db4, "db created with mvcc flag");
    $db4 = undef;

    # Test combined flags
    my $dbname5 = File::Temp->new;
    unlink $dbname5;
    my $db5 = Twom->open($dbname5, { create => 1, nocsum => 1, nosync => 1 });
    ok($db5, "db created with combined flags (nocsum + nosync)");
    $db5 = undef;
}

# Test database metadata methods
{
    my $dbname = File::Temp->new;
    unlink $dbname;
    my $db = Twom->open($dbname, { create => 1 });

    # Test fname
    my $fname = $db->fname;
    ok($fname, "fname returned a value");
    like($fname, qr/\Q$dbname\E/, "fname matches db path");

    # Test uuid
    my $uuid = $db->uuid;
    ok($uuid, "uuid returned a value");
    ok(length($uuid) > 0, "uuid is non-empty");

    # Test generation (should be 1 for new db)
    is($db->generation, 1, "generation is 1 for new db");

    # Test num_records (should be 0)
    is($db->num_records, 0, "num_records is 0 for empty db");

    # Test size
    my $size = $db->size;
    ok($size > 0, "size is positive");

    # Add a record and verify num_records changes
    $db->store("key1", "value1");
    is($db->num_records, 1, "num_records is 1 after storing one record");

    # Test sync
    my $sync_rc = $db->sync;
    ok($sync_rc >= 0, "sync succeeded");

    $db = undef;
}

# Test store/fetch with flags
{
    my $dbname = File::Temp->new;
    unlink $dbname;
    my $db = Twom->open($dbname, { create => 1 });

    # Store a key-value pair
    $db->store("testkey", "testval");

    # Test IFNOTEXIST - should fail since key exists
    eval {
        $db->store("testkey", "newval", TWOM_IFNOTEXIST);
    };
    ok($@, "store with IFNOTEXIST on existing key throws error or returns error code");

    # Verify original value is unchanged
    is($db->fetch("testkey"), "testval", "original value unchanged after failed IFNOTEXIST");

    # Test IFNOTEXIST - should succeed for new key
    my $rc = $db->store("newkey", "newval", TWOM_IFNOTEXIST);
    is($db->fetch("newkey"), "newval", "IFNOTEXIST succeeds for new key");

    # Test IFEXIST - should succeed since key exists
    $rc = $db->store("testkey", "updatedval", TWOM_IFEXIST);
    is($db->fetch("testkey"), "updatedval", "IFEXIST succeeds for existing key");

    # Test IFEXIST - should fail for non-existent key (returns error, doesn't throw)
    my $rc_ifexist = eval { $db->store("nonexistent", "val", TWOM_IFEXIST); };
    # Either it throws an error or returns without creating the key
    ok($@ || !defined($db->fetch("nonexistent")), "store with IFEXIST on non-existent key fails");

    $db = undef;
}

# Test transaction operations
{
    my $dbname = File::Temp->new;
    unlink $dbname;
    my $db = Twom->open($dbname, { create => 1 });

    # Pre-populate with some data
    $db->store("k1", "v1");
    $db->store("k2", "v2");
    $db->store("k3", "v3");

    # Test transaction fetch
    my $txn = $db->begin_txn(0);
    my $val = $txn->fetch("k1");
    is($val, "v1", "txn fetch returns correct value");

    # Test transaction store and fetch within same txn
    $txn->store("k4", "v4");
    $val = $txn->fetch("k4");
    is($val, "v4", "txn fetch returns value stored in same txn");
    $txn->commit();

    # Verify commit worked
    is($db->fetch("k4"), "v4", "value committed to db");

    # Test transaction foreach
    $txn = $db->begin_txn(0);
    my %collected;
    $txn->foreach("k", sub {
        my ($rock, $k, $v) = @_;
        $collected{$k} = $v;
        return 0;
    }, 0, "test_rock");

    is(scalar(keys %collected), 4, "txn foreach found all keys with prefix 'k'");
    is($collected{k1}, "v1", "txn foreach collected k1 correctly");
    is($collected{k4}, "v4", "txn foreach collected k4 correctly");
    $txn->commit();

    # Note: Transaction cursors have lifecycle management complexities in the C library
    # that need to be resolved before comprehensive testing. Skipping for now.

    $db = undef;
}

# Test cursor operations
{
    my $dbname = File::Temp->new;
    unlink $dbname;
    my $db = Twom->open($dbname, { create => 1 });

    # Populate with sequential data
    $db->store("a1", "val_a1");
    $db->store("a2", "val_a2");
    $db->store("a3", "val_a3");
    $db->store("b1", "val_b1");
    $db->store("b2", "val_b2");

    # Test cursor iteration
    my $cur = $db->begin_cursor("a1", 0);
    my @keys;
    while (my ($k, $v) = $cur->next) {
        push @keys, $k;
    }
    $cur->commit();
    is(scalar(@keys), 5, "cursor iterated through all keys");
    is($keys[0], "a1", "first key is a1");
    is($keys[4], "b2", "last key is b2");

    # Test cursor replace
    $cur = $db->begin_cursor("a2", 0);
    my ($k, $v) = $cur->next;
    is($k, "a2", "cursor positioned at a2");
    is($v, "val_a2", "original value is val_a2");
    $cur->replace("new_val_a2");
    $cur->commit();

    # Verify replace worked
    is($db->fetch("a2"), "new_val_a2", "cursor replace updated value");

    # Test cursor with prefix flag (CURSOR_PREFIX)
    $cur = $db->begin_cursor("a", TWOM_CURSOR_PREFIX);
    @keys = ();
    while (my ($k, $v) = $cur->next) {
        push @keys, $k;
    }
    $cur->commit();
    is(scalar(@keys), 3, "cursor with prefix flag found only 'a' keys");
    is($keys[0], "a1", "prefix cursor first key is a1");
    is($keys[2], "a3", "prefix cursor last key is a3");

    $db = undef;
}

# Test database maintenance operations
{
    my $dbname = File::Temp->new;
    unlink $dbname;
    my $db = Twom->open($dbname, { create => 1 });

    # Add some data
    for my $i (1..10) {
        $db->store("key$i", "value$i");
    }

    # Test check_consistency
    my $rc = $db->check_consistency;
    ok($rc >= 0, "check_consistency succeeded");

    # Test should_repack
    my $should = $db->should_repack;
    ok(defined($should), "should_repack returned a value");

    # Test repack
    my $gen_before = $db->generation;
    $rc = $db->repack;
    ok($rc >= 0, "repack succeeded");
    my $gen_after = $db->generation;
    ok($gen_after > $gen_before, "generation increased after repack");

    # Test dump (just verify it doesn't crash)
    $rc = $db->dump(0);
    ok($rc >= 0, "dump(0) succeeded");

    $rc = $db->dump(1);
    ok($rc >= 0, "dump(1) succeeded");

    # Test yield
    $rc = $db->yield;
    ok($rc >= 0, "yield succeeded");

    $db = undef;
}

# Test foreach with different prefixes
{
    my $dbname = File::Temp->new;
    unlink $dbname;
    my $db = Twom->open($dbname, { create => 1 });

    # Create hierarchical keys
    $db->store("user:1:name", "Alice");
    $db->store("user:1:email", 'alice@example.com');
    $db->store("user:2:name", "Bob");
    $db->store("user:2:email", 'bob@example.com');
    $db->store("admin:1:name", "Charlie");

    # Test foreach with "user:" prefix
    my %users;
    $db->foreach("user:", sub {
        my ($rock, $k, $v) = @_;
        $users{$k} = $v;
        return 0;
    });

    is(scalar(keys %users), 4, "foreach found 4 user keys");
    ok(exists $users{"user:1:name"}, "found user:1:name");
    ok(exists $users{"user:2:email"}, "found user:2:email");
    ok(!exists $users{"admin:1:name"}, "did not find admin key");

    # Test foreach with "admin:" prefix
    my %admins;
    $db->foreach("admin:", sub {
        my ($rock, $k, $v) = @_;
        $admins{$k} = $v;
        return 0;
    });

    is(scalar(keys %admins), 1, "foreach found 1 admin key");
    is($admins{"admin:1:name"}, "Charlie", "admin name is Charlie");

    # Test early termination in foreach
    my $count = 0;
    $db->foreach("user:", sub {
        my ($rock, $k, $v) = @_;
        $count++;
        return 1 if $count >= 2;  # Stop after 2 items
        return 0;
    });

    is($count, 2, "foreach stopped early when callback returned non-zero");

    $db = undef;
}

# Test multiple cursors (sequential - only one active at a time)
{
    my $dbname = File::Temp->new;
    unlink $dbname;
    my $db = Twom->open($dbname, { create => 1 });

    for my $i (1..5) {
        $db->store("item$i", "value$i");
    }

    # First cursor
    my $cur1 = $db->begin_cursor("item1", 0);
    my ($k1, $v1) = $cur1->next;
    is($k1, "item1", "cursor1 at item1");
    ($k1, $v1) = $cur1->next;
    is($k1, "item2", "cursor1 advanced to item2");
    $cur1->commit();

    # Second cursor (after first is closed)
    my $cur2 = $db->begin_cursor("item3", 0);
    my ($k2, $v2) = $cur2->next;
    is($k2, "item3", "cursor2 at item3");
    ($k2, $v2) = $cur2->next;
    is($k2, "item4", "cursor2 advanced to item4");
    $cur2->commit();

    $db = undef;
}

# Test transaction abort vs commit
{
    my $dbname = File::Temp->new;
    unlink $dbname;
    my $db = Twom->open($dbname, { create => 1 });

    $db->store("base", "value");

    # Test multiple operations in transaction with abort
    my $txn = $db->begin_txn(0);
    $txn->store("key1", "val1");
    $txn->store("key2", "val2");
    $txn->store("key3", "val3");
    $txn->abort();

    # Verify none of the changes persisted
    ok(!defined($db->fetch("key1")), "key1 not found after abort");
    ok(!defined($db->fetch("key2")), "key2 not found after abort");
    ok(!defined($db->fetch("key3")), "key3 not found after abort");

    # Test multiple operations in transaction with commit
    $txn = $db->begin_txn(0);
    $txn->store("key1", "val1");
    $txn->store("key2", "val2");
    $txn->store("key3", "val3");
    $txn->commit();

    # Verify all changes persisted
    is($db->fetch("key1"), "val1", "key1 found after commit");
    is($db->fetch("key2"), "val2", "key2 found after commit");
    is($db->fetch("key3"), "val3", "key3 found after commit");

    $db = undef;
}

# Test empty/edge cases
{
    my $dbname = File::Temp->new;
    unlink $dbname;
    my $db = Twom->open($dbname, { create => 1 });

    # Note: Empty keys are not supported by the twom library (would assert)
    # Testing only empty values and other edge cases

    # Test empty value
    $db->store("empty_val_key", "");
    is($db->fetch("empty_val_key"), "", "empty value works");

    # Test binary data (null bytes)
    my $binary_key = "key\0with\0nulls";
    my $binary_val = "val\0with\0nulls";
    $db->store($binary_key, $binary_val);
    is($db->fetch($binary_key), $binary_val, "binary data with null bytes works");

    # Test large value
    my $large_val = "x" x 10000;
    $db->store("large", $large_val);
    is(length($db->fetch("large")), 10000, "large value stored and retrieved");

    $db = undef;
}

done_testing();
