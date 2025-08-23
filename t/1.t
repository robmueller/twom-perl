use strict;
use warnings;

use Test::More;
BEGIN { use_ok('Twom') };
use File::Temp;

my $dbname = File::Temp->new;

# Open (creates if not exists)
unlink $dbname;
my $db = Twom->open($dbname, { create => 1 });
ok($db, "db created");

# Simple store/fetch
$db->store("k1", "v1");
my $v = $db->fetch("k1");
is($v, "v1", "k1 => v1");

my @v = $db->fetch("u1");
is(scalar(@v), 0, "not found returns empty list");

# Iterate with a Perl callback: sub($rock, $key, $val) -> int (0=continue, nonzero=stop)
my %m;
$db->foreach("", sub {
    my ($rock, $k, $v) = @_;
    is($rock, "context", "expected context passed");
    $m{$k} = $v;
    return 0;
}, 0, "context");

is(scalar(keys %m), 1, "a single key");
is($m{k1}, "v1", "key and value as expected");

# Transactions
my $txn = $db->begin_txn(0);
$txn->store("k2", "v2");
$txn->abort();

is($db->fetch("k2"), undef, "value not stored on abort");

$txn = $db->begin_txn(0);
$txn->store("k2", "v2");
$txn->commit();

is($db->fetch("k2"), "v2", "value stored on commit");

# Cursor
%m = ();
my $cur = $db->begin_cursor("k2", 0); # start at k2
while ( my ($ck, $cv) = $cur->next ) {
    $m{$ck} = $cv;
}
$cur->commit();

is(scalar(keys %m), 1, "a single key");
is($m{k2}, "v2", "key and value as expected");

# Header/maintenance
is($db->generation, 1, "db is generation 1");
is($db->num_records, 2, "2 records");

$db->repack;

is($db->generation, 2, "db is generation 2");

$db = undef;

done_testing();
