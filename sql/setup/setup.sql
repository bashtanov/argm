begin;
create extension argm;

-- create and analyze tables (parallel plans work only on real tables, not on SRFs)
create table test_data_1_20 as select generate_series(1,20) x;
analyze test_data_1_20;

-- force parallel execution and check if it works
do $$
declare
    t text;
begin
    perform set_config('min_parallel_relation_size', '0', true),
            set_config('parallel_setup_cost', '0', true),
            set_config('parallel_tuple_cost', '0', true),
            set_config('max_parallel_workers_per_gather', '22', true);

    for t in explain select count(*) from test_data_1_20 loop
        if t like '%Gather%' then
            -- Here we can see parallel execution is on
            return;
        end if;
    end loop;
    raise 'Looks like parallel aggregation is off';
end;
$$;
