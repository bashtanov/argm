#include "postgres.h"
#include "fmgr.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

PG_MODULE_MAGIC;

/*
 *******************************************************************************
 * Types
 *******************************************************************************
 */

typedef struct ArgmDatumWithType {
	Oid           type;
	
	/* info about datatype */
	int16         typlen;
	bool          typbyval;
	char          typalign;
	RegProcedure  cmp_proc;
	
	/* data itself */
	bool          is_null;
	Datum         value;
} ArgmDatumWithType;

typedef struct ArgmState {
	/* 
	 * element 0 of this array is payload value
	 * elements 1, 2, ... are keys to be sorted by
	 */
	ArgmDatumWithType *keys;
	int                key_count;
} ArgmState;

/*
 *******************************************************************************
 * Supplementary function headers
 *******************************************************************************
 */

static void
argm_copy_datum(bool is_null, Datum src, ArgmDatumWithType *dest, bool free);

static Datum
argm_transfn_universal(PG_FUNCTION_ARGS, int32 compareFunctionResultToAdvance);

/*
 *******************************************************************************
 * Headers for functions available in the DB
 *******************************************************************************
 */

PG_FUNCTION_INFO_V1(argmax_transfn);
PG_FUNCTION_INFO_V1(argmin_transfn);
PG_FUNCTION_INFO_V1(argm_finalfn);
PG_FUNCTION_INFO_V1(anyold_transfn);
PG_FUNCTION_INFO_V1(anyold_finalfn);

/*
 *******************************************************************************
 * Supplementary function bodies
 *******************************************************************************
 */

static void 
argm_copy_datum(bool is_null, Datum src, ArgmDatumWithType *dest, bool free)
{
	if (free && !dest->typbyval && !dest->is_null)
		pfree(DatumGetPointer(dest->value));
	
	if (is_null)
		dest->is_null = true;
	else {
		dest->is_null = false;
		if (dest->typlen == -1)
			dest->value = PointerGetDatum(PG_DETOAST_DATUM_COPY(src));
		else
			dest->value = datumCopy(src, dest->typbyval, dest->typlen);
	}
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
	int32         comparison_result;
	
	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "argm**_transfn called in non-aggregate context");
	}
	
	if (PG_ARGISNULL(0))
	{
		/* No state, first time through --- initialize */
		
		/* Make a temporary context to hold all the junk */
		oldcontext = MemoryContextSwitchTo(aggcontext);
		
		/* Initialize the state variable*/
		state = palloc(sizeof(ArgmState));
		
		state->key_count = PG_NARGS() - 1;
		state->keys = palloc(sizeof(ArgmDatumWithType) * (state->key_count));

		/* 
		 * store the info about payload and keys (arguments 1 and 2, 3, ... respectively)
		 * into state->keys ([0] and [1, 2, ...] respectively)
		 */
		for (i = 0; i < state->key_count; i++)
		{
			type = get_fn_expr_argtype(fcinfo->flinfo, i + 1);
			if (type == InvalidOid)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not determine input data type")));
			state->keys[i].type = type;
			get_typlenbyvalalign(type,
								&state->keys[i].typlen,
								&state->keys[i].typbyval,
								&state->keys[i].typalign);
			/* For keys, but not for payload, determine the sorting proc */
			if (i != 0)
				state->keys[i].cmp_proc = 
					lookup_type_cache(state->keys[i].type,
					                  TYPECACHE_CMP_PROC)->cmp_proc;

			/* Copy initial values */
			argm_copy_datum(PG_ARGISNULL(i + 1),
			                  PG_GETARG_DATUM(i + 1),
			                 &(state->keys[i]), false);
		}
	}
	else
	{
		state = (ArgmState *) PG_GETARG_POINTER(0);
		
		oldcontext = MemoryContextSwitchTo(aggcontext);
		
		/* 
		 * compare the stored keys (but not payload; that is, elements 1, 2, ... ) 
		 * and the input keys (but not payload; that is, elements 2, 3, ... )
		 * lexicographically
		 */
		for (i = 1; i < state->key_count; i++)
		{
			if (PG_ARGISNULL(i + 1) && !state->keys[i].is_null)
				/* nulls last */
				break;
			
			if (!PG_ARGISNULL(i + 1) && state->keys[i].is_null) {
				/* nulls last */
				comparison_result = 1;
			} else {
				comparison_result = DatumGetInt32(OidFunctionCall2Coll(
					state->keys[i].cmp_proc,
					PG_GET_COLLATION(),
					PG_GETARG_DATUM(i + 1),
					state->keys[i].value
				)) * compareFunctionResultToAdvance;
				
				if (comparison_result < 0)
					break;
			}
			
			if (comparison_result > 0)
			{
				/* 
				 * if we are here it means that the input data was considered
				 * to be "better" (i.e. more for argmax or less for argmin)
				 * than the stored ones
				 * 
				 * thus copy the new payloads and the keys to the state
				 */
				for (i = 0; i < state->key_count; i++)
					argm_copy_datum(PG_ARGISNULL(i + 1),
					                PG_GETARG_DATUM(i + 1),
					               &(state->keys[i]), true);
				break;
			}
		}
	}
	
	MemoryContextSwitchTo(oldcontext);
	
	PG_RETURN_POINTER(state);
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
argmin_transfn(PG_FUNCTION_ARGS)
{
	return argm_transfn_universal(fcinfo, -1);
}

Datum argm_finalfn(PG_FUNCTION_ARGS)
{
	ArgmState *state;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = (ArgmState *) PG_GETARG_POINTER(0);
	
	if (!state || state->keys[0].is_null)
		PG_RETURN_NULL();
	
	PG_RETURN_DATUM(state->keys[0].value);
}

Datum anyold_transfn(PG_FUNCTION_ARGS)
{
	Oid           type;
	Datum         state;
	MemoryContext aggcontext,
	              oldcontext;
	int16         typlen;
	bool          typbyval;
	char          typalign;
	
	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "anyold_transfn called in non-aggregate context");
	}
	
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

Datum anyold_finalfn(PG_FUNCTION_ARGS)
{
	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));
	
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
		
	PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}
