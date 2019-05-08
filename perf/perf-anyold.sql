begin;

/* plpgsql implementation of argmax*/
\ir _anyold-sql.sql

/* C implementation of argmax*/
create extension argm;

/* prepare data */
create table tbl as
select d,
	grp,
	repeat('f', 2000) || grp::text grpname
from (
	select d,
		floor(random() * 3)::int grp
	from (
		select generate_series(1, 100000/*0*/) d
		order by random()
	) _
) __;
analyze tbl;

/* show scan performance */
explain analyze select * from tbl;

/* compare 3 different methods of the same computation */
explain analyze select grp, grpname, sum(d) from tbl group by 1,2;
explain analyze select grp, min(grpname), sum(d) from tbl group by 1;
explain analyze select grp, sql_anyold(grpname), sum(d) from tbl group by 1;
explain analyze select grp, anyold(grpname), sum(d) from tbl group by 1;

rollback;
