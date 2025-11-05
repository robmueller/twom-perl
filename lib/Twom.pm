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
