package Twom;

use strict;
use warnings;
require XSLoader;
our $VERSION = '0.01';

# Export flags/ret codes if you want them in Perl space too.
use Exporter 'import';
our @EXPORT_OK = (
  # return codes
  qw(
  TWOM_OK TWOM_DONE TWOM_EXISTS TWOM_IOERROR TWOM_INTERNAL TWOM_LOCKED
  TWOM_NOTFOUND TWOM_READONLY TWOM_BADFORMAT TWOM_BADUSAGE TWOM_BADCHECKSUM
  ),
  # flags
  qw(
  TWOM_CREATE TWOM_SHARED TWOM_NOCSUM TWOM_NOSYNC TWOM_NONBLOCKING
  TWOM_ALWAYSYIELD TWOM_NOYIELD TWOM_IFNOTEXIST TWOM_IFEXIST TWOM_FETCHNEXT
  TWOM_SKIPROOT TWOM_MVCC TWOM_CURSOR_PREFIX
  TWOM_CSUM_NULL TWOM_CSUM_XXH64 TWOM_CSUM_EXTERNAL TWOM_COMPAR_EXTERNAL
  )
);

XSLoader::load(__PACKAGE__, $VERSION);

1;

package Twom::DB;    # Opaque handle
use strict; use warnings; 1;

package Twom::Txn;   # Opaque handle
use strict; use warnings; 1;

package Twom::Cursor;# Opaque handle
use strict; use warnings; 1;
