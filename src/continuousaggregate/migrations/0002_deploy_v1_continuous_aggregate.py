from django.db import migrations

from continuousaggregate.view_definitions import VIEW_STRINGS, get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('continuousaggregate', '0001_initial'),
        ('telcameras_v2', '0033_alter_personaggregate_distances'),
    ]

    _LOG_SCHEMA_NAME        = "log"
    _LOG_TABLE_NAME         = "execution_log"
    _LOG_FUNCTION_NAME      = "proc_execution_log_entry"

    _PROCESS_SCHEMA_NAME    = "prc"
    _PROCESS_FUNCTION_NAME  = "proc_pre_post_process"


    _sql_log_schema              = f"CREATE SCHEMA IF NOT EXISTS {_LOG_SCHEMA_NAME}"
    _reverse_sql_log_schema      = f"DROP SCHEMA IF EXISTS {_LOG_SCHEMA_NAME}"

    _sql_process_schema          = f"CREATE SCHEMA IF NOT EXISTS {_PROCESS_SCHEMA_NAME}"
    _reverse_sql_process_schema  = f"DROP SCHEMA IF EXISTS {_PROCESS_SCHEMA_NAME}"

    _sql_create_log_table = f"""
CREATE TABLE IF NOT EXISTS {_LOG_SCHEMA_NAME}.{_LOG_TABLE_NAME} (
  component                     varchar(250)    NULL
, component_type                varchar(50)     NULL
, parent_component              varchar(250)    NULL
, ultimate_parent_component     varchar(250)    NULL
, component_tree                varchar(500)    NULL
, regarding_object              varchar(250)    NULL
, run_id                        bigint          NULL
, eventtype                     varchar(250)    NULL
, rowcount                      bigint          NULL
, component_log_datetime        timestamp       NULL
, log_insert_datetime           timestamp       NULL
, loglevel                      int4            NULL
, summary                       varchar(100)    NULL
, description                   varchar(2000)   NULL
, execution_parameters          json            NULL
);
"""
    _reverse_sql_create_log_table  = f"DROP TABLE IF EXISTS {_LOG_SCHEMA_NAME}.{_LOG_TABLE_NAME}"

    _sql_log_function = f"""
CREATE OR REPLACE PROCEDURE {_PROCESS_SCHEMA_NAME}.{_LOG_FUNCTION_NAME}(component text, component_type text, parent_component text, ultimate_parent_component text, component_tree text, regarding_object text, run_id integer, eventtype text, rowcount integer, component_log_datetime timestamp without time zone, loglevel integer, summary text, description text, execution_parameters json)
 LANGUAGE plpgsql
AS $procedure$

declare v_sql_insert varchar;

begin

--build up insert query
v_sql_insert :=  '
                    insert into {_LOG_SCHEMA_NAME}.{_LOG_TABLE_NAME} (
                                                    component,
                                                    component_type,
                                                    parent_component,
                                                    ultimate_parent_component,
                                                    component_tree,
                                                    regarding_object,
                                                    run_id,
                                                    eventtype,
                                                    rowcount,
                                                    component_log_datetime,
                                                    log_insert_datetime,
                                                    loglevel,
                                                    summary,
                                                    description,
                                                    execution_parameters
                                                    )
                                         values (   '||''''||component ||''''||','
                                                    ||''''||component_type ||''''||','
                                                    ||''''||parent_component ||''''||','
                                                    ||''''||ultimate_parent_component ||''''||','
                                                    ||''''||component_tree ||''''||','
                                                    ||''''||regarding_object ||''''||','
                                                    ||run_id||','
                                                    ||''''||eventtype ||''''||','
                                                    ||rowcount||','
                                                    ||''''||component_log_datetime||''''||','
                                                    ||'now()::timestamp'||','
                                                    ||loglevel||','
                                                    ||''''||summary ||''''||','
                                                    ||''''||description ||''''||','
                                                    ||''''||execution_parameters ||''''
                                            ||');';

--execute insert query
execute v_sql_insert;

commit;

end;

$procedure$
;
    """

    _reverse_sql_log_function = f"DROP PROCEDURE IF EXISTS {_PROCESS_SCHEMA_NAME}.{_LOG_FUNCTION_NAME}"

    _sql_process_function = r"""
CREATE OR REPLACE PROCEDURE """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""(source_schema text, source_table text, process_schema text, target_schema text, target_table text, process_type text, implicit_deletes boolean, run_id integer, parent_component text, ultimate_parent_component text, logfromlevel integer DEFAULT 2, skip_prc_prepare boolean DEFAULT false, rebuild_spatial_index boolean DEFAULT false, run_start_datetime text DEFAULT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'::text))
 LANGUAGE plpgsql
AS $procedure$

declare
v_sql_bk_column varchar;
v_sql_pk_column varchar;
v_sql_s1_column varchar;
v_sql_calc_hash varchar;
v_sql_insert_columns_prc varchar;
v_sql_insert_columns_tgt varchar;
v_sql_select_src varchar;
v_sql_select_prc varchar;
v_prc_row_status_del int4;
v_sql_select_tgt_del varchar;
v_sql_where_tgt_del varchar;
v_sql_insert_prc_table varchar;
v_sql_update_tgt varchar;
v_sql_delete_tgt varchar;
v_sql_rowcount varchar;
v_sysrowcount int4;
v_rowcount int4;
v_sql_drop_spatial_index varchar;
v_sql_spatial_index varchar;
v_params_json json;
v_sql_debug varchar;
v_sql_tmp_tgt_key_hash varchar;
v_sql_tmp_src_key_hash varchar;
v_sql_tmp_src_tgt_action varchar;
v_sql_insert_tgt_sc varchar;
v_sql_update_tgt_sc varchar;
v_sql_delete_tgt_sc varchar;
v_source_exists bool;
v_target_exists bool;




begin



v_params_json :=  (select to_json(sub)
                from (
                    select    source_schema as source_schema
                            , source_table as source_table
                            , process_schema as process_schema
                            , target_schema as target_schema
                            , target_table as target_table
                            , process_type as process_type
                            , implicit_deletes as implicit_deletes
                            , run_id as run_id
                            , logfromlevel as logfromlevel
                            , skip_prc_prepare as skip_prc_prepare
                            , rebuild_spatial_index as rebuild_spatial_index
                            , run_start_datetime as run_start_datetime
                ) sub);

IF  process_type not in ('HH','PRE','TI','IU','SC')
    then
    call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r"""
        (
            component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type,
            component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""',
            parent_component :=  parent_component,
            ultimate_parent_component :=  ultimate_parent_component,
            component_tree := '',
            regarding_object := target_schema||'.'||target_table,
            run_id := run_id,
            eventtype := 'error',
            rowcount := -1 ,
            component_log_datetime := now()::timestamp ,
            loglevel := 4    ,
            summary := 'invalid parameters',
            description := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r""" called with invalid process parameters ',
            execution_parameters := v_params_json
            );
    raise exception '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r""" called with invalid process parameters: %',v_params_json;
end if;


if logfromlevel <= 2
    then
    call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r"""
        (
            component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type,
            component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""',
            parent_component :=  parent_component,
            ultimate_parent_component :=  ultimate_parent_component,
            component_tree := '',
            regarding_object := target_schema||'.'||target_table,
            run_id := run_id,
            eventtype := 'start',
            rowcount := -1 ,
            component_log_datetime := now()::timestamp ,
            loglevel := 2    ,
            summary := '',
            description := '',
            execution_parameters := v_params_json
        );
    commit;
end if;



v_source_exists := (select
                    case when count(0) >= 1 then true else false end
                    from information_schema.columns
                    where 1=1
                    and table_schema=source_schema
                    and table_name=source_table);

if v_source_exists=false and lower(source_schema)<>'""" + f"""{_PROCESS_SCHEMA_NAME}""" + r"""'
    then
        raise exception 'source table  %.% could not be found, check source_schema and source_table parameters',source_schema,source_table;
end if;



v_target_exists := (select
                    case when count(0) >= 1 then true else false end
                    from information_schema.columns
                    where 1=1
                    and table_schema=target_schema
                    and table_name=target_table);

if v_target_exists=false and lower(target_schema)<>'""" + f"""{_PROCESS_SCHEMA_NAME}""" + r"""'
    then
        raise exception 'target table %.% could not be found, check target_schema and target_table parameters ',target_schema,target_table ;
end if;




if  process_type in ('HH','IU')
    then

        v_sql_bk_column := (select  c_tgt.column_name from information_schema.columns c_tgt
                                    where       c_tgt.table_schema = target_schema
                                            and c_tgt.table_name = target_table
                                            and c_tgt.column_name = 'bk_'||target_table
                            );

        if v_sql_bk_column is null
        then
            raise exception 'bk_column wrong or undefined, should be formed as bk_[tablename]';
        end if;
end if;



if  process_type in ('HH','IU','TI') and upper(target_schema) = 'INT'
    then

        v_sql_s1_column := (select  c_tgt.column_name from information_schema.columns c_tgt
                                    where       c_tgt.table_schema = target_schema
                                            and c_tgt.table_name = target_table
                                            and c_tgt.column_name = 's1_'||target_table
                            );

    if v_sql_s1_column is null and process_type in ('HH','IU')
    THEN
        raise exception 's1_column wrong or undefined, should be formed as s1_[tablename]';
    end if;
end if;



if  process_type in ('SC')
    then

        v_sql_pk_column := (select  c.column_name from information_schema.table_constraints tc
                                        join information_schema.constraint_column_usage as ccu using ( constraint_schema,constraint_name)
                                        join information_schema.columns as c on ( c.table_schema = tc.constraint_schema and tc.table_name = c.table_name and ccu.column_name=c.column_name )
                                        where       tc.constraint_type = 'PRIMARY KEY'
                                            and tc.table_name = source_table
                                            and tc.table_schema = source_schema
                            );
    raise notice 'pk_column: %' , v_sql_pk_column;
    if v_sql_pk_column is null
    then
        raise exception 'pk_column not defined';
    end if;
end if;



if  process_type = ('TI')
    then
    execute 'truncate table '||target_schema||'.'||target_table || ';';
end if;


if  process_type in ('HH','IU','PRE','TI') and skip_prc_prepare = false
    THEN

    execute 'drop table if exists ' || process_schema||'.'||target_schema||'_'||target_table || ';';

    execute 'create table ' || process_schema||'.'||target_schema||'_'||target_table || ' as select *, null ::int prc_row_status from ' || target_schema || '.' || target_table || ' where 1=0;';
    execute 'alter table ' || process_schema||'.'||target_schema||'_'||target_table || ' alter column prc_row_status SET DEFAULT 0 ;';
    raise notice 'processing table created';

end if;


IF  ( process_type in ('HH','IU','TI') and skip_prc_prepare = false ) or implicit_deletes = true
    THEN
    v_sql_insert_columns_prc:= (
                                select
                                    string_agg(text('"' ||c_tgt.column_name|| '"'), ',' )
                                    ||',prc_row_status'
                                    from information_schema.columns c_tgt
                                left join information_schema.columns c_src on (c_src.table_schema = source_schema and c_src.table_name = source_table and c_src.column_name = c_tgt.column_name)
                                where c_tgt.table_schema = target_schema
                                and c_tgt.table_name = target_table

                                and c_tgt.column_name not like 'S2_%'
                                group by c_tgt.table_schema,c_tgt.table_name
                                );

end if;



IF  process_type in ('HH','IU','TI') and skip_prc_prepare = false
    THEN
    v_sql_select_src := (   select ' select distinct '||
                             string_agg(text(
                                    case
                                    when c_tgt.column_name = 'mf_insert_datetime' then 'now()::timestamp '||'"'||c_tgt.column_name||'"'
                                    when c_tgt.column_name = 'mf_update_datetime' then 'null::timestamp '||'"'||c_tgt.column_name||'"'
                                    when c_tgt.column_name = 'mf_row_hash' then 'null::uuid mf_row_hash'
                                    when c_tgt.column_name = 'mf_run_id' then run_id ||' "'||c_tgt.column_name||'"'

                                    when c_src.column_name = c_tgt.column_name then '"'||c_tgt.column_name||'"'

                                    when c_tgt.column_name = 'mf_dp_available_datetime' then ''''||run_start_datetime||''''||'::timestamp '||'"'||c_tgt.column_name||'"'
                                    when c_tgt.column_name = 'mf_deleted_ind' then 'false '||'"'||c_tgt.column_name||'"'
                                    else
                                    'null::'||c_tgt.data_type||' ' ||'"'||c_tgt.column_name||'"'
                                    end
                                        ), ',' )
                                ||
                                ',0 prc_row_status  from '||source_schema||'.'||source_table||';'
                        from    information_schema.columns c_tgt
                        left join information_schema.columns c_src on ( c_src.table_schema = source_schema and c_src.table_name = source_table and c_src.column_name = c_tgt.column_name )
                        where c_tgt.table_schema = target_schema
                            and c_tgt.table_name = target_table
                            and c_tgt.column_name not like 'S2_%'
                        group by c_tgt.table_schema,c_tgt.table_name
                        );


    execute 'insert into '|| process_schema||'.'||target_schema||'_'||target_table ||'('||v_sql_insert_columns_prc||')'||v_sql_select_src;


            if logfromlevel <=  2
                then
                GET DIAGNOSTICS v_sysrowcount := ROW_COUNT;
                v_rowcount := v_sysrowcount;
                call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r"""
                (
                    component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type,
                    component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""',
                    parent_component :=  parent_component,
                    ultimate_parent_component :=  ultimate_parent_component,
                    component_tree := '',
                    regarding_object := target_schema||'.'||target_table,
                    run_id := run_id,
                    eventtype := 'prc\insert',
                    rowcount := v_rowcount ,
                    component_log_datetime := now()::timestamp ,
                    loglevel := 2    ,
                    summary := '',
                    description := '',
                    execution_parameters := v_params_json
                );
            end if;

else raise notice 'insert into prc table skipped';
end if;



if process_type in ('HH','IU') and implicit_deletes = true
    then
    execute 'update '|| process_schema||'.'||target_schema||'_'||target_table ||' set mf_deleted_ind = false where mf_deleted_ind is null;';
end if;


if process_type in ('HH','IU') and implicit_deletes = true
    then
    if process_type = 'IU' then v_prc_row_status_del := 2;
       else v_prc_row_status_del :=1;
     end if;
    v_sql_select_tgt_del:= (    select ' select '||
                             string_agg(text(
                                    case
                                    when c_tgt.column_name = 'mf_insert_datetime' then 'now()::timestamp '||'"'||c_tgt.column_name||'"'
                                    when c_tgt.column_name = 'mf_update_datetime' then 'null::timestamp '||'"'||c_tgt.column_name||'"'
                                    when c_tgt.column_name = 'mf_row_hash' then 'null::uuid mf_row_hash'
                                    when c_tgt.column_name = 'mf_run_id' then run_id ||' "'||c_tgt.column_name||'"'
                                    when c_tgt.column_name = 'mf_dp_available_datetime' then ''''||run_start_datetime||''''||'::timestamp '||'"'||c_tgt.column_name||'"'
                                    when c_tgt.column_name = 'mf_dp_changed_datetime' then 'null::timestamp '||'"'||c_tgt.column_name||'"'
                                    when c_tgt.column_name = 'mf_deleted_ind' then 'true '||'"'||c_tgt.column_name||'"'
                                    when c_tgt.column_name = 'mf_dp_latest_int' then 'null::bool '||'"'||c_tgt.column_name||'"'

                                    when c_src.column_name = c_tgt.column_name then '"'||c_tgt.column_name||'"'
                                    else
                                    'null::'||c_tgt.data_type||' ' ||'"'||c_tgt.column_name||'"'
                                    end
                                        ), ',' )
                                ||
                                ','||v_prc_row_status_del||' prc_row_status '
                        from    information_schema.columns c_tgt
                        left join information_schema.columns c_src on ( c_src.table_schema = source_schema and c_src.table_name = source_table and c_src.column_name = c_tgt.column_name )
                        where c_tgt.table_schema = target_schema
                            and c_tgt.table_name = target_table
                            and c_tgt.column_name not like 'S2_%'
                        group by c_tgt.table_schema,c_tgt.table_name
                        );


    if  process_type = ('HH')
        then v_sql_where_tgt_del := ' and mf_dp_latest_ind = true and mf_deleted_ind = false ' ;
    else v_sql_where_tgt_del := ' and mf_deleted_ind = false ';
    end if;


    execute ' with
             tgt as ( '||v_sql_select_tgt_del||' from  ' ||target_schema||'.'||target_table||' where 1=1 '||v_sql_where_tgt_del||')
            ,src_keys as ( select distinct '||v_sql_bk_column||' from '|| process_schema||'.'||target_schema||'_'||target_table ||')
            insert into '|| process_schema||'.'||target_schema||'_'||target_table ||' ('||v_sql_insert_columns_prc||')
            select '||v_sql_insert_columns_prc||'
            from tgt
            left join src_keys using ('||v_sql_bk_column|| ')
            where src_keys.'||v_sql_bk_column||' is null;';


        if logfromlevel <=  2
            then
            GET DIAGNOSTICS v_sysrowcount :=  ROW_COUNT;
            v_rowcount = v_sysrowcount;
            call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r"""
                (
                    component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type,
                    component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""',
                    parent_component :=  parent_component,
                    ultimate_parent_component :=  ultimate_parent_component,
                    component_tree := '',
                    regarding_object := target_schema||'.'||target_table,
                    run_id := run_id,
                    eventtype := 'prc\delete',
                    rowcount := v_rowcount ,
                    component_log_datetime := now()::timestamp ,
                    loglevel := 2    ,
                    summary := '',
                    description := '',
                    execution_parameters := v_params_json
                );
            end if;
    

end if;


IF  process_type in ('HH','IU')
    then

    execute 'CREATE INDEX if not exists idx_""" + f"""{_PROCESS_SCHEMA_NAME}""" + r"""_'||v_sql_bk_column||' ON  '|| process_schema||'.'||target_schema||'_'||target_table ||' ('||v_sql_bk_column||');';

    

    raise notice 'index created on """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""" table';

end if;


IF  process_type in ('HH','IU')
    THEN
        v_sql_calc_hash :=  (
                                 select
                                'MD5(row('||string_agg(text('"' ||c_tgt.column_name|| '"::text'), ',' )||')::text)::uuid' as row_hash
                                from information_schema.columns c_tgt
                                where c_tgt.table_schema = target_schema
                                    and c_tgt.table_name = target_table

                                    and ( c_tgt.column_name not like 'mf_%' or c_tgt.column_name = 'mf_deleted_ind' )
                                    and c_tgt.column_name <> 's1_'||target_table
                                    and c_tgt.column_name <> 's2_'||target_table
                                group by c_tgt.table_schema,c_tgt.table_name
                            );


        execute 'update '|| process_schema||'.'||target_schema||'_'||target_table ||' set mf_row_hash = '||v_sql_calc_hash||' where mf_row_hash is null';
        raise notice 'hash update succeeded';
end if;


IF  process_type in ('HH')
    THEN

        execute 'update ' || process_schema||'.'||target_schema||'_'||target_table ||' as prc set prc_row_status = -1 from '||target_schema||'.'||target_table||' as tgt where tgt.'||v_sql_bk_column||'=""" + f"""{_PROCESS_SCHEMA_NAME}""" + r""".'||v_sql_bk_column||' and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".mf_dp_available_datetime = tgt.mf_dp_available_datetime and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".mf_row_hash = tgt.mf_row_hash and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".prc_row_status <> -1';

        GET DIAGNOSTICS v_sysrowcount := ROW_COUNT;
        v_rowcount = v_sysrowcount;



        execute 'update ' || process_schema||'.'||target_schema||'_'||target_table ||' as prc set prc_row_status = -1
                from (
                        select distinct '||v_sql_bk_column||'
                            , mf_dp_available_datetime
                            , mf_row_hash
                            ,case when lag(mf_row_hash) over (partition by '||v_sql_bk_column||' order by mf_dp_available_datetime) = mf_row_hash then true else false end  as duplicate
                        from '|| process_schema||'.'||target_schema||'_'||target_table ||'
                    ) as dedup
                where dedup.'||v_sql_bk_column||' = """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".'||v_sql_bk_column||' and dedup.mf_dp_available_datetime  = """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".mf_dp_available_datetime and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".prc_row_status<>-1  and dedup.duplicate=true';

       GET DIAGNOSTICS v_sysrowcount := ROW_COUNT;
        v_rowcount = v_sysrowcount;


        execute 'update ' || process_schema||'.'||target_schema||'_'||target_table ||' as """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""" set prc_row_status = -1
                from (
                        select distinct '||v_sql_bk_column||'
                            , first_value (mf_dp_available_datetime) over(partition by '||v_sql_bk_column||' order by mf_dp_available_datetime desc, mf_insert_datetime desc) mf_dp_available_datetime_latest
                            ,  first_value (mf_row_hash) over(partition by '||v_sql_bk_column||' order by mf_dp_available_datetime desc, mf_insert_datetime desc) mf_row_hash_latest
                        from '||target_schema||'.'||target_table||'
                    ) as tgt
                where tgt.'||v_sql_bk_column||'=""" + f"""{_PROCESS_SCHEMA_NAME}""" + r""".'||v_sql_bk_column||' and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".mf_dp_available_datetime > tgt.mf_dp_available_datetime_latest and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".mf_row_hash = tgt.mf_row_hash_latest and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".prc_row_status <> -1';

    GET DIAGNOSTICS v_sysrowcount :=  ROW_COUNT;
    v_rowcount = v_rowcount + v_sysrowcount;


    if logfromlevel <=  2
        then
        call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r"""
            (
                component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type,
                component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""',
                parent_component :=  parent_component,
                ultimate_parent_component :=  ultimate_parent_component,
                component_tree := '',
                regarding_object := target_schema||'.'||target_table,
                run_id := run_id,
                eventtype := 'prc\discard',
                rowcount := v_rowcount ,
                component_log_datetime := now()::timestamp ,
                loglevel := 2    ,
                summary := '',
                description := '',
                execution_parameters := v_params_json
            );
    end if;

    

end if;



IF  process_type in ('IU')
    then

    execute  'update '|| process_schema||'.'||target_schema||'_'||target_table || ' prc set prc_row_status = -1 from '||
                                target_schema||'.'|| target_table ||' tgt where 1=1 and prc.'||v_sql_bk_column||
                                '= tgt.'||v_sql_bk_column||
                                ' and prc.prc_row_status = 0 and prc.mf_row_hash = tgt.mf_row_hash; '
                                ;
    GET DIAGNOSTICS v_sysrowcount :=  ROW_COUNT;
    v_rowcount = v_sysrowcount;


    if logfromlevel <=  2
        then
        call prc.""" + f"""{_LOG_FUNCTION_NAME}""" + r"""
            (
                component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type,
                component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""',
                parent_component :=  parent_component,
                ultimate_parent_component :=  ultimate_parent_component,
                component_tree := '',
                regarding_object := target_schema||'.'||target_table,
                run_id := run_id,
                eventtype := 'prc\discard',
                rowcount := v_rowcount ,
                component_log_datetime := now()::timestamp ,
                loglevel := 2    ,
                summary := '',
                description := '',
                execution_parameters := v_params_json
            );
    end if;


    execute  'update '|| process_schema||'.'||target_schema||'_'||target_table || ' prc set prc_row_status = 2, mf_run_id = '||run_id||' from '||
                                target_schema||'.'|| target_table ||' tgt where 1=1 and prc.'||v_sql_bk_column||
                                '= tgt.'||v_sql_bk_column||
                                ' and prc.prc_row_status in (0,1) and prc.mf_row_hash <> tgt.mf_row_hash; '
                                ;
end if;




if process_type in ('IU','HH') and upper(target_schema) = 'INT'
    then
    execute ' with
            tgt_surrogate_keys as ( select distinct '||v_sql_bk_column||',' ||v_sql_s1_column||' from '||target_schema||'.'||target_table||')
            update '|| process_schema||'.'||target_schema||'_'||target_table ||' prc set '||v_sql_s1_column||' = tgt.'||v_sql_s1_column||
            ' from tgt_surrogate_keys tgt
            where """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".'||v_sql_bk_column||' = tgt.'||v_sql_bk_column||' and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".prc_row_status in (0,1,2);';
    execute 'update '|| process_schema||'.'||target_schema||'_'||target_table||' prc set '||v_sql_s1_column||' = seq_s1.seq_s1_waarde
            from ( select '|| v_sql_bk_column||', nextval('||''''||'int.seq_'||v_sql_s1_column||''''||') seq_s1_waarde
                    from (select distinct '|| v_sql_bk_column||' from '|| process_schema||'.'||target_schema||'_'||target_table|| ' where '||v_sql_s1_column||' is null and prc_row_status in (0,1)) unieke_bk
                ) seq_s1
            where 1=1 and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".'|| v_sql_bk_column||'=seq_s1.'|| v_sql_bk_column||';' ;
end if;



if process_type in ('TI') and upper(target_schema) = 'INT' and v_sql_s1_column is not null
    then
    execute 'update '|| process_schema||'.'||target_schema||'_'||target_table||' prc set '||v_sql_s1_column||' =  nextval('||''''||'int.seq_'||v_sql_s1_column||''''||') where '||v_sql_s1_column||' is null and prc_row_status in (0,1);';
end if;


IF process_type in ('HH','IU') and rebuild_spatial_index = true
    then

    v_sql_drop_spatial_index :=
                            (
                                select
                                string_agg(
                                distinct
                                            ' drop index if exists '||table_schema||'.'||table_name||'_sidx_geo_'||column_name||';','' )
                                from information_schema.columns c_tgt
                                where       c_tgt.table_schema = target_schema
                                and c_tgt.table_name = target_table
                                and c_tgt.udt_name = 'geometry'
                            );

    IF  v_sql_drop_spatial_index is not null
        then
        execute v_sql_drop_spatial_index;
    end if;
end if;



if  process_type in ('HH','IU','TI')
    then

    v_sql_insert_columns_tgt:= (
                                select
                                    '('||
                                     string_agg(text('"' ||c_tgt.column_name|| '"'), ',' )
                                    ||')'
                                from information_schema.columns c_tgt
                                where c_tgt.table_schema = target_schema
                                and c_tgt.table_name = target_table
                                and c_tgt.column_name <> 's2_'||c_tgt.table_name
                                group by c_tgt.table_schema,c_tgt.table_name
                                );



    v_sql_select_prc := (   select ' select '||
                             string_agg(text(
                                    case
                                    when c_tgt.column_name = 'mf_deleted_ind' then 'coalesce('||'"'||c_tgt.column_name||'"'||',false) '||'"'||c_tgt.column_name||'"'
                                    when c_tgt.column_name = 'mf_insert_datetime' then 'now()::timestamp '||'"'||c_tgt.column_name||'"'
                                    when c_tgt.column_name = 'mf_update_datetime' then 'now()::timestamp '||'"'||c_tgt.column_name||'"'
                                    when c_tgt.column_name = 'mf_run_id' then run_id ||' "'||c_tgt.column_name||'"'
                                    else '"'||c_tgt.column_name||'"'
                                    end
                                        ), ',' )
                                ||' from '|| process_schema||'.'||target_schema||'_'||target_table ||' where prc_row_status in (0,1);'
                        from    information_schema.columns c_tgt
                        where c_tgt.table_schema = target_schema
                            and c_tgt.table_name = target_table
                            and c_tgt.column_name <> 's2_'||c_tgt.table_name
                        group by c_tgt.table_schema,c_tgt.table_name
                        );


    execute 'insert into '||target_schema||'.'||target_table||v_sql_insert_columns_tgt||v_sql_select_prc;


    if logfromlevel <=  2
        then
        GET DIAGNOSTICS v_sysrowcount := ROW_COUNT;
        v_rowcount := v_sysrowcount;
        call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r"""
                (
                    component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type,
                    component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""',
                    parent_component :=  parent_component,
                    ultimate_parent_component :=  ultimate_parent_component,
                    component_tree := '',
                    regarding_object := target_schema||'.'||target_table,
                    run_id := run_id,
                    eventtype := 'tgt\insert',
                    rowcount := v_rowcount ,
                    component_log_datetime := now()::timestamp ,
                    loglevel := 2    ,
                    summary := '',
                    description := '',
                    execution_parameters := v_params_json
                );
       end if;

end if;



IF  process_type in ('IU')
    then
    v_sql_update_tgt :=  (
                            select  ' update '|| target_schema||'.'||target_table ||' tgt set '||
                             string_agg(
                                     '"'||c_tgt.column_name||'"'||' = src.'||'"'||c_tgt.column_name||'"'
                                        , ',' ) ||
                         ', mf_update_datetime = now()::timestamp   from '|| process_schema||'.'||target_schema||'_'||target_table || ' src where src.prc_row_status in (2) and src.'||v_sql_bk_column||' = tgt.'||v_sql_bk_column||';'
                         from   information_schema.columns c_tgt
                          where c_tgt.table_schema = target_schema
                            and c_tgt.table_name = target_table
                            and c_tgt.column_name not in ('mf_insert_datetime','mf_update_datetime', 's1_'||target_table , v_sql_bk_column)
                           group by c_tgt.table_schema, c_tgt.table_name
                            );

    execute v_sql_update_tgt;
    if logfromlevel <=  2
        then
        GET DIAGNOSTICS v_sysrowcount := ROW_COUNT;
        v_rowcount := v_sysrowcount;
        call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r"""
                (
                    component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type,
                    component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""',
                    parent_component :=  parent_component,
                    ultimate_parent_component :=  ultimate_parent_component,
                    component_tree := '',
                    regarding_object := target_schema||'.'||target_table,
                    run_id := run_id,
                    eventtype := 'tgt\update',
                    rowcount := v_rowcount ,
                    component_log_datetime := now()::timestamp ,
                    loglevel := 2    ,
                    summary := '',
                    description := '',
                    execution_parameters := v_params_json
                );
    end if;

end if;


IF  process_type in ('HH')
    then
    execute
                '
                        with
                        tgt_values as (
                                          select '|| v_sql_bk_column||'
                                            , mf_dp_available_datetime
                                            , mf_insert_datetime
                                            , ctid rowid
                                            , coalesce(lead(mf_dp_available_datetime) over (partition by '||v_sql_bk_column||' order by mf_dp_available_datetime, mf_insert_datetime,ctid),''9999-12-31''::timestamp) mf_dp_changed_datetime
                                            , case when lead(mf_dp_available_datetime) over (partition by '||v_sql_bk_column||' order by mf_dp_available_datetime, mf_insert_datetime,ctid) is null then true else false end mf_dp_latest_ind
                                            from '||target_schema||'.'||target_table||'
                                    )
                        , prc_keys as ( select distinct '||v_sql_bk_column||' from '|| process_schema||'.'||target_schema||'_'||target_table ||' where prc_row_status <> -1 )
                        update ' ||target_schema||'.'||target_table||' as tgt
                        set mf_dp_changed_datetime = tgt_values.mf_dp_changed_datetime
                            , mf_dp_latest_ind = tgt_values.mf_dp_latest_ind
                            , mf_update_datetime = now()::timestamp
                        from tgt_values
                        inner join prc_keys using ( '||v_sql_bk_column||')
                        where 1=1
                            and tgt_values.rowid= tgt.ctid
                        and ( tgt_values.mf_dp_changed_datetime <> tgt.mf_dp_changed_datetime or tgt.mf_dp_changed_datetime is null )  ;';

end if;



IF process_type in ('HH','IU','TI') and rebuild_spatial_index = true
    then

    v_sql_spatial_index :=
                            (
                                select
                                string_agg(
                                            ' create index if not exists '||table_name||'_sidx_geo_'||column_name||' on '||table_schema||'.'||table_name||' using gist ('||column_name||')',';' )
                                from information_schema.columns c_tgt
                                where       c_tgt.table_schema = target_schema
                                and c_tgt.table_name = target_table
                                and c_tgt.udt_name = 'geometry'
                                group by table_schema,table_name
                            );

    IF  v_sql_spatial_index is null
    then
    call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r"""
        (
            component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type,
            component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""',
            parent_component :=  parent_component,
            ultimate_parent_component :=  ultimate_parent_component,
            component_tree := '',
            regarding_object := target_schema||'.'||target_table,
            run_id := run_id,
            eventtype := 'warning',
            rowcount := -1 ,
            component_log_datetime := now()::timestamp ,
            loglevel := 3    ,
            summary := 'spatial index not (re)built',
            description := 'no geo column found, spatial index not (re)built',
            execution_parameters := v_params_json
            );

     else execute v_sql_spatial_index;
     end if;
end if;



IF  process_type in ('SC')
    then
    v_sql_tmp_tgt_key_hash := (
                                'drop table if exists tmp_tgt_key_hash; create temp table tmp_tgt_key_hash as select '||v_sql_pk_column||', md5(tgt.*::TEXT) rij_hash
                                from '||target_schema||'.'||target_table||' tgt;
                                alter table tmp_tgt_key_hash add primary key ('||v_sql_pk_column||');'
                                );

    execute v_sql_tmp_tgt_key_hash;
    v_sql_tmp_src_key_hash := (
                                'drop table if exists tmp_src_key_hash; create temp table tmp_src_key_hash as select '||v_sql_pk_column||', md5(src.*::TEXT) rij_hash
                                from '||source_schema||'.'||source_table||' src;
                                alter table tmp_src_key_hash add primary key ('||v_sql_pk_column||');'
                            );

    execute v_sql_tmp_src_key_hash;
    v_sql_tmp_src_tgt_action := (
                                'drop table if exists tmp_src_tgt_action; create temp table tmp_src_tgt_action as select coalesce(src.'||v_sql_pk_column||', tgt.'||v_sql_pk_column||') '||v_sql_pk_column||
                                ', case when src.'||v_sql_pk_column||' is null then '||'''delete'''||
                                       ' when tgt.'||v_sql_pk_column||' is null then '||'''insert'''||
                                       ' when src.rij_hash=tgt.rij_hash then '||'''discard'''||
                                       ' when src.rij_hash<>tgt.rij_hash then '||'''update'''||
                                ' end as db_action from tmp_src_key_hash src full outer join tmp_tgt_key_hash tgt using('||v_sql_pk_column||');'
                                );

    execute v_sql_tmp_src_tgt_action;

    v_sql_insert_tgt_sc := (
                                select ' insert into '|| target_schema||'.'||target_table ||'('||
                                  string_agg( '"'||c_src.column_name||'"',',')||') select '||
                                  string_agg( '"'||c_src.column_name||'"',',')||
                                  ' from '|| 'pte' ||'.'||source_table|| ' inner join tmp_src_tgt_action using ('||v_sql_pk_column||') where db_action= ''insert'' '
                                  from information_schema.columns c_src
                                  where table_schema= source_schema and table_name= source_table
                                  );

    execute v_sql_insert_tgt_sc;


    v_sql_update_tgt_sc :=  (
                            select  ' update '|| target_schema||'.'||target_table ||' tgt set '||
                             string_agg(
                                     '"'||c_tgt.column_name||'"'||' = src.'||'"'||c_tgt.column_name||'"'
                                        , ',' ) ||
                         ' from '|| source_schema||'.'||source_table|| ' src where 1=1 and src.'||v_sql_pk_column||' = tgt.'||v_sql_pk_column||' and tgt.'||v_sql_pk_column||' in (select '||  v_sql_pk_column||' from tmp_src_tgt_action where db_action = '||
                         '''update''' ||');'
                         from   information_schema.columns c_tgt
                          where c_tgt.table_schema = target_schema
                            and c_tgt.table_name = target_table
                            and c_tgt.column_name <> v_sql_pk_column
                           group by c_tgt.table_schema, c_tgt.table_name
                            );

    execute v_sql_update_tgt_sc;

    v_sql_delete_tgt_sc := (
                            'delete from '||target_schema||'.'||target_table
                            || ' where 1=1 and '||v_sql_pk_column||' in (select '||  v_sql_pk_column||' from tmp_src_tgt_action where db_action = '||
                            '''delete''' ||');'
                            );

    execute v_sql_delete_tgt_sc;
end if;




if logfromlevel <=  2
    then
    call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r"""
            (
                component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type,
                component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""',
                parent_component :=  parent_component,
                ultimate_parent_component :=  ultimate_parent_component,
                component_tree := '',
                regarding_object := target_schema||'.'||target_table,
                run_id := run_id,
                eventtype := 'finish',
                rowcount := -1 ,
                component_log_datetime := now()::timestamp ,
                loglevel := 2    ,
                summary := '',
                description := '',
                execution_parameters := v_params_json
            );
end if;


raise notice 'finish';

end ;
$procedure$
;
"""
    _reverse_sql_process_function = f"DROP PROCEDURE IF EXISTS {_PROCESS_SCHEMA_NAME}.{_PROCESS_FUNCTION_NAME}"

    _VIEW_NAME = "vw_cmsa_15min_v01_aggregate"
    _view_strings = get_view_strings(VIEW_STRINGS, _VIEW_NAME, indexes=[('sensor', 'timestamp_rounded')])

    _PREDICT_VIEW_NAME = "vw_cmsa_15min_v01_predict"
    _predict_view_strings = get_view_strings(VIEW_STRINGS, _PREDICT_VIEW_NAME, indexes=[('sensor', 'timestamp_rounded')])

    _REALTIME_PREDICT_VIEW_NAME = "vw_cmsa_15min_v01_realtime_predict"
    _realtime_predict_view_strings = get_view_strings(VIEW_STRINGS, _REALTIME_PREDICT_VIEW_NAME)


    operations = [
        migrations.RunSQL(
            sql=_sql_log_schema,
            reverse_sql=_reverse_sql_log_schema
        ),

        migrations.RunSQL(
            sql=_sql_process_schema,
            reverse_sql=_reverse_sql_process_schema
        ),

        migrations.RunSQL(
            sql=_sql_create_log_table,
            reverse_sql=_reverse_sql_create_log_table
        ),

        migrations.RunSQL(
            sql=_sql_log_function,
            reverse_sql=_reverse_sql_log_function
        ),

        migrations.RunSQL(
            sql=_sql_process_function,
            reverse_sql=_reverse_sql_process_function
        ),

        migrations.RunSQL(
            sql=_view_strings['sql'],
            reverse_sql=_view_strings['reverse_sql']
        ),

        migrations.RunSQL(
            sql=_predict_view_strings['sql'],
            reverse_sql=_predict_view_strings['reverse_sql']
        ),
        migrations.RunSQL(
            sql=_predict_view_strings['sql_materialized'],
            reverse_sql=_predict_view_strings['reverse_sql_materialized']
        ),
        migrations.RunSQL(
            sql=_predict_view_strings['indexes'][0]
        ),

        migrations.RunSQL(
            sql=_realtime_predict_view_strings['sql'],
            reverse_sql=_realtime_predict_view_strings['reverse_sql']
        ),
    ]
