-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION argm" to load this file. \quit

create function argmax_transfn(internal, anyelement, variadic "any") returns internal as
	'MODULE_PATHNAME', 'argmax_transfn' language c;
create function argmin_transfn(internal, anyelement, variadic "any") returns internal as
	'MODULE_PATHNAME', 'argmin_transfn' language c;
create function argm_finalfn(internal, anyelement, variadic "any") returns anyelement as
	'MODULE_PATHNAME', 'argm_finalfn' language c;
create function anyold_transfn(internal, anyelement) returns internal as
	'MODULE_PATHNAME', 'anyold_transfn' language c;
create function anyold_finalfn(internal, anyelement) returns anyelement as
	'MODULE_PATHNAME', 'anyold_transfn' language c;

create aggregate argmax(anyelement, variadic "any")
(
    sfunc = argmax_transfn,
    stype = internal,
    sspace = 128,
    finalfunc = argm_finalfn,
    finalfunc_extra
);
create aggregate argmin(anyelement, variadic "any")
(
    sfunc = argmin_transfn,
    stype = internal,
    sspace = 128,
    finalfunc = argm_finalfn,
    finalfunc_extra
);
create aggregate anyold(anyelement)
(
    sfunc = anyold_transfn,
    stype = internal,
    sspace = 8,
    finalfunc = anyold_finalfn,
    finalfunc_extra
);
