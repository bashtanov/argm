/*
 * FUNCTIONAL TESTS
 */
begin;

create extension argm;

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

select grp, argmax(txt, i, d), argmin(array[txt], (i, d)) from tbl group by grp order by grp;

select argmax(1, 2) filter (where false);

rollback;
