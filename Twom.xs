#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"

#include <stdint.h>
#include <stdbool.h>

/* Include your twom header here; adjust the include path via Makefile.PL INC */
#include "twom.h"

/* Opaque typedef aliases for typemap */
typedef struct twom_db     Twom_DB;
typedef struct twom_txn    Twom_Txn;
typedef struct twom_cursor Twom_Cursor;

/* ---- Constants ---- */
static void
twom_boot_constants(pTHX)
{
    HV *stash = NULL;

    /* return codes - still exported for error handling */
    newCONSTSUB(stash, "TWOM_OK",           newSViv(0));
    newCONSTSUB(stash, "TWOM_DONE",         newSViv(1));
    newCONSTSUB(stash, "TWOM_EXISTS",       newSViv(-1));
    newCONSTSUB(stash, "TWOM_IOERROR",      newSViv(-2));
    newCONSTSUB(stash, "TWOM_INTERNAL",     newSViv(-3));
    newCONSTSUB(stash, "TWOM_LOCKED",       newSViv(-4));
    newCONSTSUB(stash, "TWOM_NOTFOUND",     newSViv(-5));
    newCONSTSUB(stash, "TWOM_READONLY",     newSViv(-6));
    newCONSTSUB(stash, "TWOM_BADFORMAT",    newSViv(-7));
    newCONSTSUB(stash, "TWOM_BADUSAGE",     newSViv(-8));
    newCONSTSUB(stash, "TWOM_BADCHECKSUM",  newSViv(-9));

    /* flags are no longer exported - use hashref options instead */
}

/* ---- Helpers ---- */
#define CROAK_ON_NEG(rc, what) \
    STMT_START { if ((rc) < 0 && (rc) != TWOM_NOTFOUND) \
        croak("%s failed: %s (%d)", (what), twom_strerror((rc)), (rc)); } STMT_END

/* foreach() Perl callback trampoline */
typedef struct {
    SV *cb;   /* coderef */
    SV *rock; /* any SV passed by caller */
} PerlCB;

static int
xs_twom_cb(void *rock,
           const char *key, size_t keylen,
           const char *data, size_t datalen)
{
    dTHX;
    dSP;

    PerlCB *ctx = (PerlCB*)rock;
    int rc = 0;

    ENTER; SAVETMPS;

    PUSHMARK(SP);
    XPUSHs(ctx->rock ? ctx->rock : &PL_sv_undef);
    XPUSHs(sv_2mortal(newSVpvn(key,  (STRLEN)keylen)));
    XPUSHs(sv_2mortal(newSVpvn(data, (STRLEN)datalen)));
    PUTBACK;

    /* Call in scalar context; return int (0 to continue, non-zero to stop) */
    int count = call_sv(ctx->cb, G_SCALAR);
    SPAGAIN;
    if (count > 0) {
        SV *svret = POPs;
        if (SvOK(svret)) rc = (int)SvIV(svret);
    }
    PUTBACK;

    FREETMPS; LEAVE;
    return rc;
}

/* Default error handler: warn. You can change this later if needed. */
static void
xs_twom_error(const char *msg, const char *fmt, ...)
{
    va_list ap;
    SV *sv = newSVpv("", 0);
    sv_catpv(sv, msg ? msg : "twom");
    sv_catpv(sv, ": ");

    va_start(ap, fmt);
    PerlIO *io = IoOFP(GvIOp(PL_stderrgv));
    if (fmt && *fmt) {
        STRLEN n;
        char buf[1024];
        vsnprintf(buf, sizeof(buf), fmt, ap);
        sv_catpv(sv, buf);
    }
    va_end(ap);

    sv_catpv(sv, "\n");
    Perl_warn(aTHX_ "%s", SvPV_nolen(sv));
    SvREFCNT_dec(sv);
}

/* Flag parsing helper */
typedef struct {
    const char *name;
    UV flag;
} FlagMap;

static UV
parse_flags(pTHX_ SV *flags_sv, const FlagMap *valid_flags, size_t num_flags, const char *context)
{
    UV flags = 0;

    /* If not defined, return 0 */
    if (!flags_sv || !SvOK(flags_sv)) {
        return 0;
    }

    /* If it's a hashref, parse it */
    if (SvROK(flags_sv) && SvTYPE(SvRV(flags_sv)) == SVt_PVHV) {
        HV *hv = (HV*)SvRV(flags_sv);
        HE *entry;

        hv_iterinit(hv);
        while ((entry = hv_iternext(hv))) {
            I32 keylen;
            const char *key = hv_iterkey(entry, &keylen);
            SV *val = hv_iterval(hv, entry);

            /* Skip if value is false */
            if (!SvTRUE(val)) continue;

            /* Find matching flag */
            bool found = false;
            for (size_t i = 0; i < num_flags; i++) {
                if (strlen(valid_flags[i].name) == (size_t)keylen &&
                    strncmp(valid_flags[i].name, key, (size_t)keylen) == 0) {
                    flags |= valid_flags[i].flag;
                    found = true;
                    break;
                }
            }

            if (!found) {
                croak("%s: invalid flag '%.*s'", context, (int)keylen, key);
            }
        }
    } else {
        croak("%s: flags must be a hashref", context);
    }

    return flags;
}

#define PARSE_FLAGS(sv, map, ctx) parse_flags(aTHX_ (sv), (map), sizeof(map)/sizeof((map)[0]), (ctx))

/* ---- XS ---- */

MODULE = Twom           PACKAGE = Twom

BOOT:
    {
        HV *stash = gv_stashpv("Twom", GV_ADD);
        twom_boot_constants(aTHX);
    }

PROTOTYPES: ENABLE

void
open(class, fname, opts=NULL)
    const char *class
    const char *fname
    SV *opts
  PPCODE:
    {
        struct twom_open_data setup = TWOM_OPEN_DATA_INITIALIZER;
        struct twom_db *db = NULL;
        UV flags = 0;

        /* Valid flags for open */
        static const FlagMap open_flags[] = {
            {"create",       TWOM_CREATE},
            {"shared",       TWOM_SHARED},
            {"nocsum",       TWOM_NOCSUM},
            {"nosync",       TWOM_NOSYNC},
            {"nonblocking",  TWOM_NONBLOCKING},
            {"csum_null",    TWOM_CSUM_NULL},
            {"csum_xxh64",   TWOM_CSUM_XXH64},
        };

        if (opts && SvOK(opts)) {
            flags = PARSE_FLAGS(opts, open_flags, "open");
        }

        setup.flags = (uint32_t)flags;
        setup.compar = NULL;
        setup.csum   = NULL;
        setup.error  = xs_twom_error;

        int rc = twom_db_open(fname, &setup, &db, NULL);
        CROAK_ON_NEG(rc, "twom_db_open");

        EXTEND(SP, 1);
        PUSHs(sv_2mortal(sv_setref_pv(newSV(0), "Twom::DB",  (void*)db)));
    }

const char *
strerror(code)
    int code
  CODE:
    RETVAL = twom_strerror(code);
  OUTPUT:
    RETVAL

MODULE = Twom          PACKAGE = Twom::DB

void
DESTROY(self)
    SV *self
  CODE:
    Twom_DB *db = INT2PTR(Twom_DB*, SvIV(SvRV(self)));
    if (db) {
        twom_db_close(&db);
        sv_setiv(SvRV(self), PTR2IV(db)); /* NULL it */
    }

SV *
fname(db)
    Twom_DB *db
  CODE:
    RETVAL = newSVpv(twom_db_fname(db), 0);
  OUTPUT: RETVAL

SV *
uuid(db)
    Twom_DB *db
  CODE:
    RETVAL = newSVpv(twom_db_uuid(db), 0);
  OUTPUT: RETVAL

UV
generation(db)
    Twom_DB *db
  CODE:
    RETVAL = (UV)twom_db_generation(db);
  OUTPUT: RETVAL

UV
num_records(db)
    Twom_DB *db
  CODE:
    RETVAL = (UV)twom_db_num_records(db);
  OUTPUT: RETVAL

UV
size(db)
    Twom_DB *db
  CODE:
    RETVAL = (UV)twom_db_size(db);
  OUTPUT: RETVAL

int
sync(db)
    Twom_DB *db
  CODE:
    RETVAL = twom_db_sync(db);
  OUTPUT: RETVAL

int
check_consistency(db)
    Twom_DB *db
  CODE:
    RETVAL = twom_db_check_consistency(db);
  OUTPUT: RETVAL

int
repack(db)
    Twom_DB *db
  CODE:
    RETVAL = twom_db_repack(db);
  OUTPUT: RETVAL

int
should_repack(db)
    Twom_DB *db
  CODE:
    RETVAL = (int)twom_db_should_repack(db);
  OUTPUT: RETVAL

int
yield(db)
    Twom_DB *db
  CODE:
    RETVAL = twom_db_yield(db);
  OUTPUT: RETVAL

void
fetch(db, key_sv, opts=NULL)
    Twom_DB *db
    SV *key_sv
    SV *opts
  PPCODE:
    {
        static const FlagMap fetch_flags[] = {
            {"fetchnext", TWOM_FETCHNEXT},
        };
        UV flags = PARSE_FLAGS(opts, fetch_flags, "fetch");

        STRLEN klen;
        const char *k = SvPV(key_sv, klen);
        const char *keyp = NULL, *valp = NULL;
        size_t keylen = 0, vallen = 0;
        int rc = twom_db_fetch(db, k, (size_t)klen, &keyp, &keylen, &valp, &vallen, (int)flags);
        if (rc == TWOM_NOTFOUND || rc == TWOM_DONE) XSRETURN_EMPTY;
        CROAK_ON_NEG(rc, "twom_db_fetch");

        EXTEND(SP, 1);
        PUSHs(sv_2mortal(newSVpvn(valp, (STRLEN)vallen)));
    }

int
store(db, key_sv, val_sv, opts=NULL)
    Twom_DB *db
    SV *key_sv
    SV *val_sv
    SV *opts
  CODE:
    {
        static const FlagMap store_flags[] = {
            {"ifnotexist", TWOM_IFNOTEXIST},
            {"ifexist",    TWOM_IFEXIST},
            {"nosync",     TWOM_NOSYNC},
        };
        UV flags = PARSE_FLAGS(opts, store_flags, "store");

        STRLEN klen, vlen;
        const char *k = SvPV(key_sv, klen);
        const char *v = SvPV(val_sv, vlen);
        int rc = twom_db_store(db, k, (size_t)klen, v, (size_t)vlen, (int)flags);
        CROAK_ON_NEG(rc, "twom_db_store");
        RETVAL = rc; /* usually 0 */
    }
  OUTPUT: RETVAL

int
dump(db, detail = 0)
    Twom_DB *db
    int detail
  CODE:
    RETVAL = twom_db_dump(db, detail);
  OUTPUT: RETVAL

void
foreach(db, prefix_sv, cb, opts=NULL, rock=&PL_sv_undef)
    Twom_DB *db
    SV *prefix_sv
    SV *cb
    SV *opts
    SV *rock
  PPCODE:
    {
        static const FlagMap foreach_flags[] = {
            {"alwaysyield", TWOM_ALWAYSYIELD},
            {"noyield",     TWOM_NOYIELD},
            {"skiproot",    TWOM_SKIPROOT},
        };
        UV flags = PARSE_FLAGS(opts, foreach_flags, "foreach");

        if (!SvROK(cb) || SvTYPE(SvRV(cb)) != SVt_PVCV) {
            croak("foreach requires a coderef");
        }
        STRLEN plen;
        const char *prefix = SvPV(prefix_sv, plen);

        PerlCB ctx;
        ctx.cb   = SvREFCNT_inc_NN(cb);
        ctx.rock = SvREFCNT_inc_NN(rock);

        int rc = twom_db_foreach(db, prefix, (size_t)plen,
                                 NULL /* p */, xs_twom_cb, (void*)&ctx, (int)flags);

        SvREFCNT_dec(ctx.cb);
        SvREFCNT_dec(ctx.rock);

        CROAK_ON_NEG(rc, "twom_db_foreach");
        /* Return nothing on success (typical iterator style) */
        XSRETURN_EMPTY;
    }

Twom_Txn *
begin_txn(db, shared = 0)
    Twom_DB *db
    int shared
  CODE:
    {
        struct twom_txn *txn = NULL;
        int rc = twom_db_begin_txn(db, shared, &txn);
        CROAK_ON_NEG(rc, "twom_db_begin_txn");
        RETVAL = txn;
    }
  OUTPUT: RETVAL

Twom_Cursor *
begin_cursor(db, key_sv, opts=NULL)
    Twom_DB *db
    SV *key_sv
    SV *opts
  CODE:
    {
        static const FlagMap cursor_flags[] = {
            {"mvcc",          TWOM_MVCC},
            {"cursor_prefix", TWOM_CURSOR_PREFIX},
            {"noyield",       TWOM_NOYIELD},
        };
        UV flags = PARSE_FLAGS(opts, cursor_flags, "begin_cursor");

        STRLEN klen; const char *k = SvPV(key_sv, klen);
        struct twom_cursor *cur = NULL;
        int rc = twom_db_begin_cursor(db, k, (size_t)klen, &cur, (int)flags);
        CROAK_ON_NEG(rc, "twom_db_begin_cursor");
        RETVAL = cur;
    }
  OUTPUT: RETVAL

MODULE = Twom        PACKAGE = Twom::Txn

void
DESTROY(self)
    SV *self
  CODE:
    Twom_Txn *txn = INT2PTR(Twom_Txn*, SvIV(SvRV(self)));
    if (txn) {
        twom_txn_abort(&txn);                  /* best effort */
        sv_setiv(SvRV(self), PTR2IV(txn));     /* NULL it regardless */
    }

int
commit(self)
    SV *self
  CODE:
    Twom_Txn *txn = INT2PTR(Twom_Txn*, SvIV(SvRV(self)));
    RETVAL = twom_txn_commit(&txn);            /* txn may be set to NULL */
    if (RETVAL >= 0)                           /* only write-back on success */
        sv_setiv(SvRV(self), PTR2IV(txn));     /* store NULL back into object */
  OUTPUT: RETVAL

int
abort(self)
    SV *self
  CODE:
    Twom_Txn *txn = INT2PTR(Twom_Txn*, SvIV(SvRV(self)));
    RETVAL = twom_txn_abort(&txn);
    if (RETVAL >= 0)
        sv_setiv(SvRV(self), PTR2IV(txn));     /* NULL on success */
  OUTPUT: RETVAL

int
yield(txn)
    Twom_Txn *txn
  CODE:
    RETVAL = twom_txn_yield(txn);
  OUTPUT: RETVAL

void
fetch(txn, key_sv, opts=NULL)
    Twom_Txn *txn
    SV *key_sv
    SV *opts
  PPCODE:
    {
        static const FlagMap fetch_flags[] = {
            {"fetchnext", TWOM_FETCHNEXT},
        };
        UV flags = PARSE_FLAGS(opts, fetch_flags, "txn fetch");

        STRLEN klen;
        const char *k = SvPV(key_sv, klen);
        const char *keyp = NULL, *valp = NULL;
        size_t keylen = 0, vallen = 0;
        int rc = twom_txn_fetch(txn, k, (size_t)klen, &keyp, &keylen, &valp, &vallen, (int)flags);
        if (rc == TWOM_NOTFOUND || rc == TWOM_DONE) XSRETURN_EMPTY;
        CROAK_ON_NEG(rc, "twom_txn_fetch");

        EXTEND(SP, 1);
        PUSHs(sv_2mortal(newSVpvn(valp, (STRLEN)vallen)));
    }

int
store(txn, key_sv, val_sv, opts=NULL)
    Twom_Txn *txn
    SV *key_sv
    SV *val_sv
    SV *opts
  CODE:
    {
        static const FlagMap store_flags[] = {
            {"ifnotexist", TWOM_IFNOTEXIST},
            {"ifexist",    TWOM_IFEXIST},
        };
        UV flags = PARSE_FLAGS(opts, store_flags, "txn store");

        STRLEN klen, vlen;
        const char *k = SvPV(key_sv, klen);
        const char *v = SvPV(val_sv, vlen);
        int rc = twom_txn_store(txn, k, (size_t)klen, v, (size_t)vlen, (int)flags);
        CROAK_ON_NEG(rc, "twom_txn_store");
        RETVAL = rc;
    }
  OUTPUT: RETVAL

void
foreach(txn, prefix_sv, cb, opts=NULL, rock=&PL_sv_undef)
    Twom_Txn *txn
    SV *prefix_sv
    SV *cb
    SV *opts
    SV *rock
  PPCODE:
    {
        static const FlagMap foreach_flags[] = {
            {"alwaysyield", TWOM_ALWAYSYIELD},
            {"noyield",     TWOM_NOYIELD},
            {"skiproot",    TWOM_SKIPROOT},
            {"mvcc",        TWOM_MVCC},
        };
        UV flags = PARSE_FLAGS(opts, foreach_flags, "txn foreach");

        if (!SvROK(cb) || SvTYPE(SvRV(cb)) != SVt_PVCV) {
            croak("foreach requires a coderef");
        }
        STRLEN plen;
        const char *prefix = SvPV(prefix_sv, plen);

        PerlCB ctx;
        ctx.cb   = SvREFCNT_inc_NN(cb);
        ctx.rock = SvREFCNT_inc_NN(rock);

        int rc = twom_txn_foreach(txn, prefix, (size_t)plen,
                                  NULL /* p */, xs_twom_cb, (void*)&ctx, (int)flags);

        SvREFCNT_dec(ctx.cb);
        SvREFCNT_dec(ctx.rock);

        CROAK_ON_NEG(rc, "twom_txn_foreach");
        XSRETURN_EMPTY;
    }

Twom_Cursor *
begin_cursor(txn, key_sv, opts=NULL)
    Twom_Txn *txn
    SV *key_sv
    SV *opts
  CODE:
    {
        static const FlagMap cursor_flags[] = {
            {"mvcc",          TWOM_MVCC},
            {"cursor_prefix", TWOM_CURSOR_PREFIX},
            {"noyield",       TWOM_NOYIELD},
        };
        UV flags = PARSE_FLAGS(opts, cursor_flags, "txn begin_cursor");

        STRLEN klen; const char *k = SvPV(key_sv, klen);
        struct twom_cursor *cur = NULL;
        int rc = twom_txn_begin_cursor(txn, k, (size_t)klen, &cur, (int)flags);
        CROAK_ON_NEG(rc, "twom_txn_begin_cursor");
        RETVAL = cur;
    }
  OUTPUT: RETVAL

MODULE = Twom         PACKAGE = Twom::Cursor

void
DESTROY(self)
    SV *self
  CODE:
    Twom_Cursor *cur = INT2PTR(Twom_Cursor*, SvIV(SvRV(self)));
    if (cur) {
        twom_cursor_abort(&cur);
        sv_setiv(SvRV(self), PTR2IV(cur));
    }

int
commit(self)
    SV *self
  CODE:
    Twom_Cursor *cur = INT2PTR(Twom_Cursor*, SvIV(SvRV(self)));
    RETVAL = twom_cursor_commit(&cur);
    if (RETVAL >= 0)
        sv_setiv(SvRV(self), PTR2IV(cur));
  OUTPUT: RETVAL

int
abort(self)
    SV *self
  CODE:
    Twom_Cursor *cur = INT2PTR(Twom_Cursor*, SvIV(SvRV(self)));
    RETVAL = twom_cursor_abort(&cur);
    if (RETVAL >= 0)
        sv_setiv(SvRV(self), PTR2IV(cur));
  OUTPUT: RETVAL

void
next(cur)
    Twom_Cursor *cur
  PPCODE:
    {
        const char *keyp = NULL, *valp = NULL;
        size_t keylen = 0, vallen = 0;
        int rc = twom_cursor_next(cur, &keyp, &keylen, &valp, &vallen);
        if (rc == TWOM_DONE || rc == TWOM_NOTFOUND) XSRETURN_EMPTY;
        CROAK_ON_NEG(rc, "twom_cursor_next");
        EXTEND(SP, 2);
        PUSHs(sv_2mortal(newSVpvn(keyp, (STRLEN)keylen)));
        PUSHs(sv_2mortal(newSVpvn(valp, (STRLEN)vallen)));
    }

int
replace(cur, val_sv, opts=NULL)
    Twom_Cursor *cur
    SV *val_sv
    SV *opts
  CODE:
    {
        static const FlagMap replace_flags[] = {
            {"ifnotexist", TWOM_IFNOTEXIST},
            {"ifexist",    TWOM_IFEXIST},
        };
        UV flags = PARSE_FLAGS(opts, replace_flags, "cursor replace");

        STRLEN vlen; const char *v = SvPV(val_sv, vlen);
        int rc = twom_cursor_replace(cur, v, (size_t)vlen, (int)flags);
        CROAK_ON_NEG(rc, "twom_cursor_replace");
        RETVAL = rc;
    }
  OUTPUT: RETVAL

