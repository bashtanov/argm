-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION argm" to load this file. \quit

create function argmax_transfn(internal, anyelement, variadic "any") returns internal as
       'MODULE_PATHNAME', 'argmax_transfn' language c immutable;
create function argmax_combine(internal, internal) returns internal as
       'MODULE_PATHNAME', 'argmax_combine' language c immutable;
create function argmin_transfn(internal, anyelement, variadic "any") returns internal as
       'MODULE_PATHNAME', 'argmin_transfn' language c immutable;
create function argmin_combine(internal, internal) returns internal as
       'MODULE_PATHNAME', 'argmin_combine' language c immutable;
create function argm_finalfn(internal, anyelement, variadic "any") returns anyelement as
       'MODULE_PATHNAME', 'argm_finalfn' language c immutable;
create function argm_serial(internal) returns bytea as
       'MODULE_PATHNAME', 'argm_serial' language c immutable strict;
create function argm_deserial(bytea, internal) returns internal as
       'MODULE_PATHNAME', 'argm_deserial' language c immutable strict;

create function anyold_transfn(anyelement, anyelement) returns anyelement as
       'MODULE_PATHNAME', 'anyold_transfn' language c immutable strict;

create aggregate argmax(anyelement, variadic "any")
(
       sfunc = argmax_transfn,
       stype = internal,
       sspace = 128,
       finalfunc = argm_finalfn,
       finalfunc_extra,
       combinefunc = argmax_combine,
       serialfunc = argm_serial,
       deserialfunc = argm_deserial,
       parallel = safe
);

create aggregate argmin(anyelement, variadic "any")
(
       sfunc = argmin_transfn,
       stype = internal,
       sspace = 128,
       finalfunc = argm_finalfn,
       finalfunc_extra,
       combinefunc = argmin_combine,
       serialfunc = argm_serial,
       deserialfunc = argm_deserial,
       parallel = safe
);

create aggregate anyold(anyelement)
(
       sfunc = anyold_transfn,
       stype = anyelement,
       combinefunc = anyold_transfn,
       parallel = safe
);
