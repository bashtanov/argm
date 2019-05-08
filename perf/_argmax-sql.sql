/* plpgsql implementation of one-column argmax */

create type ty_argmax_date_text_state as (
	d_date date,
	t_payload text
);

create function fn_argmax_date_text_greater(p_ty_state ty_argmax_date_text_state, p_t_payload text, p_d_date date)
returns ty_argmax_date_text_state as $$
	select case when p_d_date <= p_ty_state.d_date
	            then p_ty_state
	            else (p_d_date, p_t_payload)::ty_argmax_date_text_state
	       end;
$$ language sql;

create function fn_argmax_date_text_extract_payload(p_ty_state ty_argmax_date_text_state)
returns text as $$
	select p_ty_state.t_payload;
$$ language sql;

create aggregate sql_argmax(text, date) (
	sfunc = fn_argmax_date_text_greater,
	stype = ty_argmax_date_text_state,
	finalfunc = fn_argmax_date_text_extract_payload
);
