#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

PG_MODULE_MAGIC;

/*
 *******************************************************************************
 * Macros
 *******************************************************************************
 */

#define GET_AGG_CONTEXT(fcinfo, aggcontext)  \
    if (! AggCheckCallContext(fcinfo, &aggcontext)) {   \
        elog(ERROR, \
             "argm extension function called in non-aggregate context");  \
    }

#define CHECK_AGG_CONTEXT(fcinfo)  \
    if (! AggCheckCallContext(fcinfo, NULL)) {   \
        elog(ERROR, \
             "argm extension function called in non-aggregate context");  \
    }

#if PG_VERSION_NUM < 110000

#define pq_sendint16(buf, value)   \
    pq_sendint(buf, value, 2)

#define pq_sendint32(buf, value)   \
    pq_sendint(buf, value, 4)

#endif

#define pq_getmsgint16(buf)   \
    pq_getmsgint(buf, 2)

#define pq_getmsgint32(buf)   \
    pq_getmsgint(buf, 4)

/*
 *******************************************************************************
 * Types
 *******************************************************************************
 */

typedef struct ArgmDatumMetadata {
    Oid           type;

    /* info about datatype */
    int16         typlen;
    bool          typbyval;
    char          typalign;
    RegProcedure  cmp_proc;
} ArgmDatumMetadata;

typedef struct ArgmDatumWithMetadata {
    ArgmDatumMetadata metadata;
    
    /* data itself */
    bool          is_null;
    Datum         value;
} ArgmDatumWithMetadata;

typedef struct ArgmState {
    /*
     * element 0 of this array is payload value
     * elements 1, 2, ... are keys to be sorted by
     */
    ArgmDatumWithMetadata *keys;
    short                  key_count; /* FunctionCallInfoData.nargs is short */
} ArgmState;

/*
 *******************************************************************************
 * Supplementary function headers
 *******************************************************************************
 */

static ArgmState *init_state(short key_count);

static void argm_copy_datum(bool is_null, Datum src, 
                            ArgmDatumWithMetadata *dest, bool free);

static void copy_state_keys(ArgmState *state_dst, ArgmState *state_src);

static int compare_one_key(ArgmDatumWithMetadata *old,
                           bool new_is_null, Datum new_datum,
                           Oid collation, int compareFunctionResultToAdvance);

static Datum argm_transfn_universal(PG_FUNCTION_ARGS,
                                    int32 compareFunctionResultToAdvance);

static Datum argm_combine_universal(PG_FUNCTION_ARGS,
                                    int32 compareFunctionResultToAdvance);

static bytea *datum2binary(Datum datum, Oid type);

static Datum binary2datum(StringInfo buf, Oid type);
/*
 *******************************************************************************
 * Headers for functions available in the DB
 *******************************************************************************
 */

PG_FUNCTION_INFO_V1(argmax_transfn);
PG_FUNCTION_INFO_V1(argmin_transfn);
PG_FUNCTION_INFO_V1(argmax_combine);
PG_FUNCTION_INFO_V1(argmin_combine);
PG_FUNCTION_INFO_V1(argm_finalfn);
PG_FUNCTION_INFO_V1(argm_serial);
PG_FUNCTION_INFO_V1(argm_deserial);
PG_FUNCTION_INFO_V1(anyold_transfn);
PG_FUNCTION_INFO_V1(anyold_finalfn);

/*
 *******************************************************************************
 * Supplementary function bodies
 *******************************************************************************
 */

static ArgmState *init_state(short key_count)
{
    ArgmState *state = palloc(sizeof(ArgmState));

    state->key_count = key_count;
    state->keys = palloc(sizeof(ArgmDatumWithMetadata) * key_count);
    return state;
}

static void
argm_copy_datum(bool is_null, Datum src, ArgmDatumWithMetadata *dest, bool free)
{
    if (free && !dest->metadata.typbyval && !dest->is_null)
        pfree(DatumGetPointer(dest->value));

    if (is_null)
        dest->is_null = true;
    else {
        dest->is_null = false;
        if (dest->metadata.typlen == -1)
            dest->value = PointerGetDatum(PG_DETOAST_DATUM_COPY(src));
        else
            dest->value = datumCopy(src, dest->metadata.typbyval,
                                    dest->metadata.typlen);
    }
}

static void copy_state_keys(ArgmState *state_dst, ArgmState *state_src)
{
    int i;

    /* Raw copying is good for metadata, null flags, and typbyval data */
    memcpy(state_dst->keys, state_src->keys,
           state_src->key_count * sizeof(ArgmDatumWithMetadata));

    /* However, we have to copy non-null non-typbyval data properly */
    for (i = 0; i < state_src->key_count; i++)
    {
        ArgmDatumWithMetadata *key_src = &state_src->keys[i];
        ArgmDatumWithMetadata *key_dst = &state_dst->keys[i];
        if (!key_src->is_null && !key_src->metadata.typbyval)
            argm_copy_datum(false, key_src->value, key_dst, false);
    }
}
/*
 * compares old and new,
 * returns 1 when new is better, 0 on parity, -1 when old is better
 */
static int compare_one_key(ArgmDatumWithMetadata *old,
            bool new_is_null, Datum new_datum,
            Oid collation, int compareFunctionResultToAdvance)
{
    if (new_is_null && old->is_null)
        return 0;
    /* nulls last */
    if (new_is_null && !old->is_null)
        return -1;
    /* nulls last */
    if (!new_is_null && old->is_null)
        return 1;

    return DatumGetInt32(OidFunctionCall2Coll(
        old->metadata.cmp_proc,
        collation,
        new_datum,
        old->value
    )) * compareFunctionResultToAdvance;
}
/*
 * The function args are the following:
 *   0 -- state (internal type)
 *   1 -- payload
 *   2, 3, ... -- keys
 */
static Datum
argm_transfn_universal(PG_FUNCTION_ARGS, int32 compareFunctionResultToAdvance)
{
    Oid           type;
    ArgmState    *state;
    MemoryContext aggcontext,
                  oldcontext;
    int           i;
    bool          need_copy,
                  need_free_old;

    GET_AGG_CONTEXT(fcinfo, aggcontext);

    /* Make a temporary context to hold all the junk */
    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0))
    {
        /* No state, first time through --- initialize */
        state = init_state(PG_NARGS() - 1);

        /*
         * store the info about
         * payload and keys (arguments 1 and 2, 3, ... respectively)
         * into state->keys ([0] and [1], [2], ... respectively)
         */
        for (i = 0; i < state->key_count; i++)
        {
            type = get_fn_expr_argtype(fcinfo->flinfo, i + 1);
            if (type == InvalidOid)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("could not determine input data type")));
            state->keys[i].metadata.type = type;
            get_typlenbyvalalign(type,
                                &state->keys[i].metadata.typlen,
                                &state->keys[i].metadata.typbyval,
                                &state->keys[i].metadata.typalign);
            /* For keys, but not for payload, determine the sorting proc */
            if (i != 0)
                state->keys[i].metadata.cmp_proc =
                    lookup_type_cache(state->keys[i].metadata.type,
                                      TYPECACHE_CMP_PROC)->cmp_proc;
        }

        /* Copy initial values */
        need_copy = true;
        need_free_old = false;
    }
    else
    {
        int preference; /* -1 for old, 1 for new */

        state = (ArgmState *) PG_GETARG_POINTER(0);

        /*
         * compare lexicographically
         * stored keys (but not payload; that is, elements 1, 2, ... )
         * and input keys (but not payload; that is, elements 2, 3, ... )
         */
        /* other things being equal, preserve existing one */
        need_copy = false;
        
        for (i = 1; i < state->key_count; i++)
        {
            preference = compare_one_key(
                &(state->keys[i]),
                PG_ARGISNULL(i + 1),
                PG_GETARG_DATUM(i + 1),
                PG_GET_COLLATION(),
                compareFunctionResultToAdvance
            );
            /* no need to compare further values, keep current state */
            if (preference == -1)
                break;
            /* no need to compare further values, copy new state */
            if (preference == 1) {
                need_copy = true;
                need_free_old = true;
                break;
            }
        }
    }

    /* 
     * promote the new one
     * if it is strictly better than old or there's no old
     */
    if (need_copy)
    {
        for (i = 0; i < state->key_count; i++)
            argm_copy_datum(PG_ARGISNULL(i + 1),
                            PG_GETARG_DATUM(i + 1),
                            &(state->keys[i]),
                            need_free_old);
    }

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(state);
}

static Datum
argm_combine_universal(PG_FUNCTION_ARGS, int32 compareFunctionResultToAdvance)
{
    MemoryContext aggcontext,
                  oldcontext;
    ArgmState *state0,
              *state1;

    GET_AGG_CONTEXT(fcinfo, aggcontext);

    if (PG_ARGISNULL(1))
        PG_RETURN_POINTER(PG_GETARG_POINTER(0));
    
    oldcontext = MemoryContextSwitchTo(aggcontext);

    state1 = (ArgmState *) PG_GETARG_POINTER(1);

    if (PG_ARGISNULL(0))
    {
        state0 = palloc(sizeof(ArgmState));
        
        /* Copy data (1 --> 0) across memory contexts */
        state0 = init_state(state1->key_count);
        copy_state_keys(state0, state1);
    }
    else
    {
        int preference = 0;
        int i;
        state0 = (ArgmState *) PG_GETARG_POINTER(0);

        /*
         * compare the stored keys of state0 and state1 lexicographically
         */
        for (i = 1; i < state0->key_count; i++)
        {
            preference = compare_one_key(
                &(state0->keys[i]),
                state1->keys[i].is_null,
                state1->keys[i].value,
                PG_GET_COLLATION(),
                compareFunctionResultToAdvance
            );
            /* no need to compare further values, as we have a defined result */
            if (preference != 0)
                break;
        }
        /* copy state1 into state0 if the former is better */
        if (preference == 1)
            copy_state_keys(state0, state1);
    }

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(state0);
}

static bytea *datum2binary(Datum datum, Oid type)
{
    Oid typiofunc;
    bool typisvarlena;

    getTypeBinaryOutputInfo(type, &typiofunc, &typisvarlena);
    return OidSendFunctionCall(typiofunc, datum);
}

static Datum binary2datum(StringInfo buf, Oid type)
{
    Oid typiofunc;
    Oid typioparam;

    getTypeBinaryInputInfo(type, &typiofunc, &typioparam);
    return OidReceiveFunctionCall(typiofunc, buf, typioparam, -1);
}

/*
 *******************************************************************************
 * Bodies for functions available in the DB
 *******************************************************************************
 */

Datum
argmax_transfn(PG_FUNCTION_ARGS)
{
    return argm_transfn_universal(fcinfo, 1);
}

Datum
argmax_combine(PG_FUNCTION_ARGS)
{
    return argm_combine_universal(fcinfo, 1);
}

Datum
argmin_transfn(PG_FUNCTION_ARGS)
{
    return argm_transfn_universal(fcinfo, -1);
}

Datum
argmin_combine(PG_FUNCTION_ARGS)
{
    return argm_combine_universal(fcinfo, -1);
}

Datum argm_finalfn(PG_FUNCTION_ARGS)
{
    ArgmState *state;

    /* cannot be called directly because of internal-type argument */
    CHECK_AGG_CONTEXT(fcinfo);

    state = (ArgmState *) PG_GETARG_POINTER(0);

	if (!state || state->keys[0].is_null)
		PG_RETURN_NULL();
	
    PG_RETURN_DATUM(state->keys[0].value);
}

Datum argm_serial(PG_FUNCTION_ARGS)
{
    StringInfoData buf;
    ArgmState *state = (ArgmState *) PG_GETARG_POINTER(0);
    int i;

    CHECK_AGG_CONTEXT(fcinfo);

    pq_begintypsend(&buf);
    pq_sendint16(&buf, state->key_count);

    for (i = 0; i < state->key_count; i++)
    {
        bytea *outputbytes;
        ArgmDatumWithMetadata* key = &state->keys[i];
        
        pq_sendbytes(&buf, (char *)(&key->metadata), sizeof(key->metadata));

        if (key->is_null)
        {
            /* emit -1 data length to signify a NULL */
            pq_sendint32(&buf, -1);
            continue;
        }
        /* Convert the column value to binary */
        outputbytes = datum2binary(key->value, key->metadata.type);

        pq_sendint32(&buf, VARSIZE(outputbytes) - VARHDRSZ);
        pq_sendbytes(&buf, VARDATA(outputbytes),
                     VARSIZE(outputbytes) - VARHDRSZ);
    }
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));

}

Datum argm_deserial(PG_FUNCTION_ARGS)
{
    bytea *sstate = PG_GETARG_BYTEA_PP(0);
    StringInfoData buf;
    ArgmState *state;
    int i;

    CHECK_AGG_CONTEXT(fcinfo);

    /*
     * Copy the bytea into a StringInfo so that we can "receive" it using the
     * standard recv-function infrastructure.
     */
    initStringInfo(&buf);
    appendBinaryStringInfo(&buf,
                           VARDATA_ANY(sstate), VARSIZE_ANY_EXHDR(sstate));

    state = init_state(pq_getmsgint16(&buf));

    for (i = 0; i < state->key_count; i++)
    {
        ArgmDatumWithMetadata* key = &state->keys[i];

        int itemlen;
        StringInfoData item_buf;
        char csave;

        key->metadata = *(ArgmDatumMetadata *)
                        pq_getmsgbytes(&buf, sizeof(key->metadata));
        itemlen = pq_getmsgint32(&buf);
        if (itemlen < -1 || itemlen > (buf.len - buf.cursor))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                     errmsg("insufficient data left in message")));
        if (itemlen == -1)
        {
            /* -1 data length means NULL */
            key->is_null = true;
            continue;
        }

        key->is_null = false;

        /*
         * Rather than copying data around, we just set up a phony
         * StringInfo pointing to the correct portion of the input buffer.
         * We assume we can scribble on the input buffer so as to maintain
         * the convention that StringInfos have a trailing null.
         */
        item_buf.data = &buf.data[buf.cursor];
        item_buf.maxlen = itemlen + 1;
        item_buf.len = itemlen;
        item_buf.cursor = 0;
        buf.cursor += itemlen;
        csave = buf.data[buf.cursor];
        buf.data[buf.cursor] = '\0';

        key->value = binary2datum(&item_buf, key->metadata.type);

        /* Trouble if it didn't eat the whole buffer */
        if (item_buf.cursor != itemlen)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                     errmsg("improper binary format in argm state key %d", i)));

        /* restore the cheekily overwritten byte */
        buf.data[buf.cursor] = csave;
    }

    PG_RETURN_POINTER(state);
}

/*
 * This function is semantically equivalent to a two-argument coalesce, but
 * saves on not copying anything if it returns its first argument
 */
Datum anyold_transfn(PG_FUNCTION_ARGS)
{
    Oid           type;
    Datum         state;
    MemoryContext aggcontext,
                  oldcontext;
    int16         typlen;
    bool          typbyval;
    char          typalign;

    GET_AGG_CONTEXT(fcinfo, aggcontext);

    if (PG_ARGISNULL(0))
    {
        if (PG_ARGISNULL(1))
            PG_RETURN_NULL();
        /* First non-null value --- initialize */
        oldcontext = MemoryContextSwitchTo(aggcontext);

        type = get_fn_expr_argtype(fcinfo->flinfo, 1);
        if (type == InvalidOid)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("could not determine input data type")));
        get_typlenbyvalalign(type,
                            &typlen,
                            &typbyval,
                            &typalign);

        /* Copy initial value */
        if (typlen == -1)
            state = PointerGetDatum(PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(1)));
        else
            state = datumCopy(PG_GETARG_DATUM(1), typbyval, typlen);

        MemoryContextSwitchTo(oldcontext);
    }
    else
    {
        state = PG_GETARG_DATUM(0);
    }

    PG_RETURN_DATUM(state);
}
