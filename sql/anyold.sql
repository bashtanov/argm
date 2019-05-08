/*
 * FUNCTIONAL TESTS
 */
begin;

create extension argm;

create table tbl2 as 
select d,
       i,
       grp::text || repeat('1', 10) txt,
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

analyze tbl2;

select grp, anyold(txt), sum(i) from tbl2 group by grp order by grp;

rollback;
