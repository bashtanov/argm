-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION argm UPDATE TO '1.1.0'" to load this file. \quit
do $$ begin
       raise 'Sorry, argm upgrade was not implemented. Please DROP and CREATE EXTENSION instead';
end; $$;
