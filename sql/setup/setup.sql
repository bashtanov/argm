begin;
create extension argm;

-- create and analyze tables (parallel plans work only on real tables, not on SRFs)
create table test_data_1_20 as select generate_series(1,20) x;
analyze test_data_1_20;
create table test_data_1_200k as select generate_series(1,200000) x;
analyze test_data_1_200k;

-- force parallel execution and check if it works
do $$
declare
    t text;
begin
    -- 9.6 and 10+ have different paramater names
    perform set_config(param_name, '0', true)
    from unnest('{
       min_parallel_relation_size,
       min_parallel_table_scan_size,
       min_parallel_index_scan_size,
       parallel_setup_cost,
       parallel_tuple_cost
    }'::text[]) param_name
    where param_name in (select name from pg_settings);

    perform set_config('max_parallel_workers_per_gather', '22', true);

    for t in explain select count(*) from test_data_1_20 loop
        if t like '%Gather%' then
            -- Here we can see parallel execution is on
            return;
        end if;
    end loop;
    raise 'Looks like parallel aggregation is off';
end;
$$;
