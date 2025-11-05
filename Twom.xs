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
    HV *stash = gv_stashpv("Twom", GV_ADD);

    /* return codes */
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

    /* flags */
    newCONSTSUB(stash, "TWOM_CREATE",          newSVuv(1u<<0));
    newCONSTSUB(stash, "TWOM_SHARED",          newSVuv(1u<<1));
    newCONSTSUB(stash, "TWOM_NOCSUM",          newSVuv(1u<<2));
    newCONSTSUB(stash, "TWOM_NOSYNC",          newSVuv(1u<<3));
    newCONSTSUB(stash, "TWOM_NONBLOCKING",     newSVuv(1u<<4));
    newCONSTSUB(stash, "TWOM_ALWAYSYIELD",     newSVuv(1u<<9));
    newCONSTSUB(stash, "TWOM_NOYIELD",         newSVuv(1u<<10));
    newCONSTSUB(stash, "TWOM_IFNOTEXIST",      newSVuv(1u<<11));
    newCONSTSUB(stash, "TWOM_IFEXIST",         newSVuv(1u<<12));
    newCONSTSUB(stash, "TWOM_FETCHNEXT",       newSVuv(1u<<13));
    newCONSTSUB(stash, "TWOM_SKIPROOT",        newSVuv(1u<<14));
    newCONSTSUB(stash, "TWOM_MVCC",            newSVuv(1u<<15));
    newCONSTSUB(stash, "TWOM_CURSOR_PREFIX",   newSVuv(1u<<16));
    newCONSTSUB(stash, "TWOM_CSUM_NULL",       newSVuv(1u<<27));
    newCONSTSUB(stash, "TWOM_CSUM_XXH64",      newSVuv(1u<<28));
    newCONSTSUB(stash, "TWOM_CSUM_EXTERNAL",   newSVuv(1u<<29));
    newCONSTSUB(stash, "TWOM_COMPAR_EXTERNAL", newSVuv(1u<<30));
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

/* ---- XS ---- */

MODULE = Twom           PACKAGE = Twom

BOOT:
    {
        HV *stash = gv_stashpv("Twom", GV_ADD);
        twom_boot_constants(aTHX);
    }

PROTOTYPES: ENABLE

void
open(class, fname, opts=Nullhv)
    const char *class
    const char *fname
    HV *opts
  PPCODE:
    {
        struct twom_open_data setup = TWOM_OPEN_DATA_INITIALIZER;
        struct twom_db *db = NULL;
        UV flags = 0;

        if (opts) {
            SV **svp;
            if ((svp = hv_fetch(opts, "flags", 5, 0)) && SvOK(*svp))
                flags |= SvUV(*svp);

            /* convenience booleans */
            const struct { const char *k; UV f; } map[] = {
                {"create",       1u<<0},
                {"shared",       1u<<1},
                {"nocsum",       1u<<2},
                {"nosync",       1u<<3},
                {"nonblocking",  1u<<4},
                {"alwaysyield",  1u<<9},
                {"noyield",      1u<<10},
                {"ifnotexist",   1u<<11},
                {"ifexist",      1u<<12},
                {"fetchnext",    1u<<13},
                {"skiproot",     1u<<14},
                {"mvcc",         1u<<15},
                {"cursor_prefix",1u<<16},
                /* Checksumming/compar are deliberately NOT enabled via opts */
            };
            for (unsigned i=0; i<sizeof(map)/sizeof(map[0]); i++) {
                SV **v = hv_fetch(opts, map[i].k, (I32)strlen(map[i].k), 0);
                if (v && SvTRUE(*v)) flags |= map[i].f;
            }
        }

        /* Refuse external compar/csum from Perl for now (no rock/context). */
        if (flags & ((1u<<29) | (1u<<30))) {
            croak("TWOM_CSUM_EXTERNAL/TWOM_COMPAR_EXTERNAL are not supported from Perl XS safely");
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
fetch(db, key_sv, flags = 0)
    Twom_DB *db
    SV *key_sv
    int flags
  PPCODE:
    {
        STRLEN klen;
        const char *k = SvPV(key_sv, klen);
        const char *keyp = NULL, *valp = NULL;
        size_t keylen = 0, vallen = 0;
        int rc = twom_db_fetch(db, k, (size_t)klen, &keyp, &keylen, &valp, &vallen, flags);
        if (rc == TWOM_NOTFOUND || rc == TWOM_DONE) XSRETURN_EMPTY;
        CROAK_ON_NEG(rc, "twom_db_fetch");

        EXTEND(SP, 1);
        PUSHs(sv_2mortal(newSVpvn(valp, (STRLEN)vallen)));
    }

int
store(db, key_sv, val_sv, flags = 0)
    Twom_DB *db
    SV *key_sv
    SV *val_sv
    int flags
  CODE:
    {
        STRLEN klen, vlen;
        const char *k = SvPV(key_sv, klen);
        const char *v = SvPV(val_sv, vlen);
        int rc = twom_db_store(db, k, (size_t)klen, v, (size_t)vlen, flags);
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
foreach(db, prefix_sv, cb, flags = 0, rock = &PL_sv_undef)
    Twom_DB *db
    SV *prefix_sv
    SV *cb
    int flags
    SV *rock
  PPCODE:
    {
        if (!SvROK(cb) || SvTYPE(SvRV(cb)) != SVt_PVCV) {
            croak("foreach requires a coderef");
        }
        STRLEN plen;
        const char *prefix = SvPV(prefix_sv, plen);

        PerlCB ctx;
        ctx.cb   = SvREFCNT_inc_NN(cb);
        ctx.rock = SvREFCNT_inc_NN(rock);

        int rc = twom_db_foreach(db, prefix, (size_t)plen,
                                 NULL /* p */, xs_twom_cb, (void*)&ctx, flags);

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
begin_cursor(db, key_sv, flags = 0)
    Twom_DB *db
    SV *key_sv
    int flags
  CODE:
    {
        STRLEN klen; const char *k = SvPV(key_sv, klen);
        struct twom_cursor *cur = NULL;
        int rc = twom_db_begin_cursor(db, k, (size_t)klen, &cur, flags);
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
fetch(txn, key_sv, flags = 0)
    Twom_Txn *txn
    SV *key_sv
    int flags
  PPCODE:
    {
        STRLEN klen;
        const char *k = SvPV(key_sv, klen);
        const char *keyp = NULL, *valp = NULL;
        size_t keylen = 0, vallen = 0;
        int rc = twom_txn_fetch(txn, k, (size_t)klen, &keyp, &keylen, &valp, &vallen, flags);
        if (rc == TWOM_NOTFOUND || rc == TWOM_DONE) XSRETURN_EMPTY;
        CROAK_ON_NEG(rc, "twom_txn_fetch");

        EXTEND(SP, 1);
        PUSHs(sv_2mortal(newSVpvn(valp, (STRLEN)vallen)));
    }

int
store(txn, key_sv, val_sv, flags = 0)
    Twom_Txn *txn
    SV *key_sv
    SV *val_sv
    int flags
  CODE:
    {
        STRLEN klen, vlen;
        const char *k = SvPV(key_sv, klen);
        const char *v = SvPV(val_sv, vlen);
        int rc = twom_txn_store(txn, k, (size_t)klen, v, (size_t)vlen, flags);
        CROAK_ON_NEG(rc, "twom_txn_store");
        RETVAL = rc;
    }
  OUTPUT: RETVAL

void
foreach(txn, prefix_sv, cb, flags = 0, rock = &PL_sv_undef)
    Twom_Txn *txn
    SV *prefix_sv
    SV *cb
    int flags
    SV *rock
  PPCODE:
    {
        if (!SvROK(cb) || SvTYPE(SvRV(cb)) != SVt_PVCV) {
            croak("foreach requires a coderef");
        }
        STRLEN plen;
        const char *prefix = SvPV(prefix_sv, plen);

        PerlCB ctx;
        ctx.cb   = SvREFCNT_inc_NN(cb);
        ctx.rock = SvREFCNT_inc_NN(rock);

        int rc = twom_txn_foreach(txn, prefix, (size_t)plen,
                                  NULL /* p */, xs_twom_cb, (void*)&ctx, flags);

        SvREFCNT_dec(ctx.cb);
        SvREFCNT_dec(ctx.rock);

        CROAK_ON_NEG(rc, "twom_txn_foreach");
        XSRETURN_EMPTY;
    }

Twom_Cursor *
begin_cursor(txn, key_sv, flags = 0)
    Twom_Txn *txn
    SV *key_sv
    int flags
  CODE:
    {
        STRLEN klen; const char *k = SvPV(key_sv, klen);
        struct twom_cursor *cur = NULL;
        int rc = twom_txn_begin_cursor(txn, k, (size_t)klen, &cur, flags);
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
replace(cur, val_sv, flags = 0)
    Twom_Cursor *cur
    SV *val_sv
    int flags
  CODE:
    {
        STRLEN vlen; const char *v = SvPV(val_sv, vlen);
        int rc = twom_cursor_replace(cur, v, (size_t)vlen, flags);
        CROAK_ON_NEG(rc, "twom_cursor_replace");
        RETVAL = rc;
    }
  OUTPUT: RETVAL

