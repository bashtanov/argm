/*
 * FUNCTIONAL TESTS
 */
\i sql/setup/setup.sql

create table tbl as 
select d,
       i,
       grp::text || '-' || d || '-' || repeat(i::text, 10) txt,
       grp
from (
       select date'2011-11-11' - di d,
              i
       from generate_series(1, 9) i
       cross join generate_series(1, 8) di
       order by random()
) _
cross join (values
       (1),
       (2),
       (3)
) grp (grp);

analyze tbl;

select grp, argmax(txt, i, d), argmin(array[txt], 1, i, d) from tbl group by grp order by grp;
-- TODO: rewrite like this after anonymous record types handling is fixed
-- select grp, argmax(txt, i, d), argmin(array[txt], (i, d)) from tbl group by grp order by grp;

select argmax(1, 2) filter (where false);

-- This way a problem with combine function called with both arguments nulls was reproduced.
-- A bit of luck needed, as it relies on scan order.
select argmax(x, x) filter (where x = 1) from test_data_1_200k;
select argmax(x, x) filter (where x = 100000) from test_data_1_200k;
select argmax(x, x) filter (where x = 200000) from test_data_1_200k;

-- string comparison test
select argmax(x::text, x::text) = max(x::text) are_equal from (select relname x from pg_class) _;

\i sql/setup/teardown.sql
