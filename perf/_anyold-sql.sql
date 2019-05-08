/* plpgsql implementation of one-column argmax */

create function frst (text, text) returns text as $$ select $1; $$ language sql;

create aggregate sql_anyold(text) (
	sfunc = frst,
	stype = text
);
