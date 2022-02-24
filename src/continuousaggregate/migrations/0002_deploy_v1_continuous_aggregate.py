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
CREATE OR REPLACE PROCEDURE """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""(
  source_schema text
, source_table text
, process_schema text
, target_schema text
, target_table text
, process_type text
, implicit_deletes boolean
, run_id integer
, parent_component text
, ultimate_parent_component text
, logfromlevel integer DEFAULT 2
, skip_prc_prepare boolean DEFAULT false
, rebuild_spatial_index boolean DEFAULT false
, run_start_datetime text DEFAULT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'::text)
, debug_mode bool default false
)
LANGUAGE plpgsql
AS $procedure$ 
/*******************************************************************************************************
 * proc_pre_post_process voert technische dataverwerking (historie) uit conform opgegeven verwerkingstype
 * voorbeeld aanroep onder code
 * versie   door    notes
 * 1.0              initial
 * 1.1              bugfix deleted indicator voor INT verwerking
 * 1.2              voor HH verwerking deduplicatie toegeveoegd voor opvolgende rijen in de tijd met zelfde hash
 * 1.3              impliciete deletes voor IU row status 2 (update) 
 * 1.4              bugfix om te zorgen dat deleted indicator meegenomen wordt in hash berekening
 * 1.5              verbetering aanroep log procedure voor zodat voor pre_post verwerking onderscheid gemaakt wordt in de PRE en post (IU/HH/TI) verwerking.
 * 1.6              TI S1 sleutel ook vanuit proces vullen, facultatief, in  tegenstelling tot bij IU en HH niet verplictht
 * 1.7              Toevoegen verwerkingsmethode SC, deze verwerking synchroniseert 2 tabellen, enige vereiste is enkelvoudige primary key
 * 1.8              Toevoegen check op bestaan van bron en doeltabel
 * 1.9      CoenE   Added debug_mode by which you can show the queries to execute as output 
******************************************************************************************************/
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
v_stmt text;

/****************** prc_rowstatus values:*************
 * -1 : discard in proces
 * -2 : physical delete in target
 * 0 : insert (initial value) 
 * 1 : insert delete records
 * 2 : update in target
 ****************************************************/


begin

--create json from used parameters  
v_params_json :=  (
    select to_json(sub)
    from (
        select    
          source_schema         as source_schema
        , source_table          as source_table
        , process_schema        as process_schema
        , target_schema         as target_schema
        , target_table          as target_table
        , process_type          as process_type
        , implicit_deletes      as implicit_deletes
        , run_id                as run_id
        , logfromlevel          as logfromlevel
        , skip_prc_prepare      as skip_prc_prepare
        , rebuild_spatial_index as rebuild_spatial_index
        , run_start_datetime    as run_start_datetime
        , debug_mode            as debug_mode
    ) sub
);        
         
if  process_type not in ('HH','PRE','TI','IU','SC')
    then
    v_stmt = '
    -----------------------------------------
    /* Log error into execution log */

    call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
      component := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type||'''
    , component_type := ''proc_pre_post_process''
    , parent_component := '''||parent_component||'''
    , ultimate_parent_component := '''||ultimate_parent_component||'''
    , component_tree := ''''
    , regarding_object := '''||target_schema||'.'||target_table||'''
    , run_id := '||run_id||'
    , eventtype := ''error''
    , rowcount := -1
    , component_log_datetime := '''||now()::timestamp||'''
    , loglevel := 4
    , summary := ''invalid parameters''
    , description := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r""" called with invalid process parameters''
    , execution_parameters := '''||v_params_json||'''
    );
    raise exception ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r""" called with invalid process parameters: %'','||v_params_json||'
    ;
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else
            -- Unfortunately we can't use the "execute v_stmt;" statement because a transaction within transaction is not allowed. So below statement is identical to above.
            -- I've tried to avoid this by using a prc.proc_execution_log_entry function but then we can't use the command "execute" in combination with a variable. Also tried this with "perform" insead of "execute".
            call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
              component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type
            , component_type := 'proc_pre_post_process'
            , parent_component := parent_component
            , ultimate_parent_component := ultimate_parent_component
            , component_tree := ''
            , regarding_object := target_schema||'.'||target_table
            , run_id := run_id
            , eventtype := 'error'
            , rowcount := -1
            , component_log_datetime := now()::timestamp
            , loglevel := 4
            , summary := 'invalid parameters'
            , description := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r""" called with invalid process parameters'
            , execution_parameters := v_params_json 
            );
            raise exception '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r""" called with invalid process parameters: %', v_params_json;
    end if;

end if;


--log start into execution log 
if logfromlevel <= 2
    then
    v_stmt = '
    -----------------------------------------
    /* Log start into execution log */

    call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
      component := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type||'''
    , component_type := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""''
    , parent_component := '''||parent_component||'''
    , ultimate_parent_component := '''||ultimate_parent_component||'''
    , component_tree := ''''
    , regarding_object := '''||target_schema||'.'||target_table||'''
    , run_id := '||run_id||'
    , eventtype := ''start''
    , rowcount := -1
    , component_log_datetime := '''||now()::timestamp||'''
    , loglevel := 2
    , summary := ''''
    , description := ''''
    , execution_parameters := '''||v_params_json||'''
    );
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else
            -- Unfortunately we can't use the "execute v_stmt;" statement because a transaction within transaction is not allowed. So below statement is identical to above.
            -- I've tried to avoid this by using a prc.proc_execution_log_entry function but then we can't use the command "execute" in combination with a variable. Also tried this with "perform" insead of "execute".
            call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
              component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type
            , component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""'
            , parent_component := parent_component
            , ultimate_parent_component := ultimate_parent_component
            , component_tree := ''
            , regarding_object := target_schema||'.'||target_table
            , run_id := run_id
            , eventtype := 'start'
            , rowcount := -1
            , component_log_datetime := now()::timestamp
            , loglevel := 2
            , summary := ''
            , description := ''
            , execution_parameters := v_params_json 
            );
    end if;

end if;


--check existance source , except for prc schema
v_source_exists := (select 
                    case when count(0) >= 1 then true else false end
                    from information_schema.columns
                    where 1=1
                    and table_schema=source_schema
                    and table_name=source_table);

if v_source_exists=false and lower(source_schema) <> '""" + f"""{_PROCESS_SCHEMA_NAME}""" + r"""'
    then
        raise exception 'source table  %.% could not be found, check source_schema and source_table parameters',source_schema,source_table;
end if;


--check existance target 
v_target_exists := (select 
                    case when count(0) >= 1 then true else false end
                    from information_schema.columns
                    where 1=1
                    and table_schema=target_schema
                    and table_name=target_table);

if v_target_exists=false and lower(target_schema) <> '""" + f"""{_PROCESS_SCHEMA_NAME}""" + r"""'
    then
        raise exception 'target table %.% could not be found, check target_schema and target_table parameters ',target_schema,target_table ;
end if;



--get bk column (functional key)
if  process_type in ('HH','IU') 
    then
        --get bk_column
        v_sql_bk_column := (select  c_tgt.column_name from information_schema.columns c_tgt
                                    where       c_tgt.table_schema = target_schema
                                            and c_tgt.table_name = target_table
                                            and c_tgt.column_name = 'bk_'||target_table
                            );
        --raise notice 'bk_column: %' , v_sql_bk_column;
        if v_sql_bk_column is null 
        then
            raise exception 'bk_column wrong or undefined, should be formed as bk_[tablename]';
        end if;
end if;


--get surrogate key 1, the one that is 1:1 with functional key and does not change historically
if  process_type in ('HH','IU','TI') and upper(target_schema) = 'INT'
    then
        --get s1_column
        v_sql_s1_column := (select  c_tgt.column_name from information_schema.columns c_tgt
                                    where       c_tgt.table_schema = target_schema
                                            and c_tgt.table_name = target_table
                                            and c_tgt.column_name = 's1_'||target_table
                            );
    --raise notice 's1_column: %' , v_sql_s1_column;
    if v_sql_s1_column is null and process_type in ('HH','IU')
    then
        raise exception 's1_column wrong or undefined, should be formed as s1_[tablename]';
    end if;
end if;


-- Get pk column (primary key)
if  process_type in ('SC') 
    then
        -- Get bk_column
        v_sql_pk_column := (
            select c.column_name 
            from information_schema.table_constraints           as tc
            join information_schema.constraint_column_usage     as ccu  using (constraint_schema,constraint_name)
            join information_schema.columns                     as c    on (    c.table_schema = tc.constraint_schema 
                                                                            and tc.table_name = c.table_name 
                                                                            and ccu.column_name=c.column_name) 
            where tc.constraint_type = 'PRIMARY KEY'
            and tc.table_name = source_table
            and tc.table_schema = source_schema
        );
    raise notice 'pk_column: %' , v_sql_pk_column;
    if v_sql_pk_column is null 
    then
        raise exception 'pk_column not defined';
    end if;
end if;

    
-- Truncate target table for     
if  process_type = ('TI') 
    then
    v_stmt = '
    -----------------------------------------
    /* Truncate target table because of process_type ''TI'' */

    truncate table '||target_schema||'.'||target_table||';
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;

end if;

--create prc table if needed
if  process_type in ('HH','IU','PRE','TI') and skip_prc_prepare = false
    then
    v_stmt = '
    -----------------------------------------    
    /* Create processing table */

    /* Drop processing table if it exists and create it according to target structure + prc_row_status_column */
    drop table if exists '||process_schema||'.'||target_schema||'_'||target_table || ';
    
    -- Create processing table based on DDL from target table
    create table '||process_schema||'.'||target_schema||'_'||target_table||' as 
        select
          *
        , null::int as prc_row_status 
        from '||target_schema||'.'||target_table||'
        where 1=0;

    -- Set prc_row_status default to value 0 (insert new row)
    alter table '||process_schema||'.'||target_schema||'_'||target_table||'
        alter column prc_row_status 
        set default 0;
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
        raise notice 'Processing table created';
    end if;

end if;

-- Create column set for insert in prc (used for insert when prepare is true or in case of implicit_deletes
if  ( process_type in ('HH','IU','TI') and skip_prc_prepare = false ) or implicit_deletes = true
    then 
    v_sql_insert_columns_prc := (
        select 
        ltrim(
        string_agg(text(', "' ||c_tgt.column_name|| '"'), E'\n') 
        ||E'\n, "prc_row_status"\n'
        , ',')
        from information_schema.columns         as c_tgt
        left join information_schema.columns    as c_src on (   c_src.table_schema = source_schema 
                                                            and c_src.table_name = source_table 
                                                            and c_src.column_name = c_tgt.column_name)
        where c_tgt.table_schema = target_schema
        and c_tgt.table_name = target_table
        --S2 column always generated
        and c_tgt.column_name not like 'S2_%'
        group by 
          c_tgt.table_schema
        , c_tgt.table_name
    );
    --raise notice 'insert columns prc: %',v_sql_insert_columns_prc;
end if;


-- Generate and execute insert to processing
if  process_type in ('HH','IU','TI') and skip_prc_prepare = false
    then
    v_sql_select_src := (   
        select
        E'\n select distinct \n '|| 
         ltrim(
            string_agg(text( 
        case
          when c_tgt.column_name = 'mf_insert_datetime'       then ', now()::timestamp as '||'"'||c_tgt.column_name||'"'
          when c_tgt.column_name = 'mf_update_datetime'       then ', null::timestamp as '||'"'||c_tgt.column_name||'"'
          when c_tgt.column_name = 'mf_row_hash'              then ', null::uuid      as mf_row_hash'
          when c_tgt.column_name = 'mf_run_id'                then ', '||run_id ||' as "'||c_tgt.column_name||'"'
          -- Use columns from source when available
          when c_src.column_name = c_tgt.column_name          then ', "'||c_tgt.column_name||'"'
          -- When not provided the available datetime is the insert datetime
          when c_tgt.column_name = 'mf_dp_available_datetime' then ', '''||run_start_datetime||''''||'::timestamp as '||'"'||c_tgt.column_name||'"'
          when c_tgt.column_name = 'mf_deleted_ind'           then ', false as '||'"'||c_tgt.column_name||'"'
          else ', null::'||c_tgt.data_type||' as ' ||'"'||c_tgt.column_name||'"'
        end
            ), E'\n' )
        ||
        E'\n, 0 as prc_row_status \n from '||source_schema||'.'||source_table||';'
        , ',')
        from information_schema.columns         as c_tgt
        left join information_schema.columns    as c_src on (       c_src.table_schema = source_schema
                                                                and c_src.table_name = source_table 
                                                                and c_src.column_name = c_tgt.column_name )
        where c_tgt.table_schema = target_schema
        and c_tgt.table_name = target_table
        and c_tgt.column_name not like 'S2_%'
        group by 
          c_tgt.table_schema
        , c_tgt.table_name
    );
    --raise notice 'select on src: %',v_sql_select_src; 
    --raise notice '%',process_schema;
    
    v_stmt = '
    -----------------------------------------
    /*  Generate and execute insert to processing
        Column list for prc insert is generated basede on the postgres information_schema
    */

    insert into '||process_schema||'.'||target_schema||'_'||target_table||E' (\n '
    ||v_sql_insert_columns_prc||
    ')'||
    v_sql_select_src||
    '
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;
    
            -- Log inserted rows into execution log 
            if logfromlevel <=  2
                then
                GET DIAGNOSTICS v_sysrowcount := ROW_COUNT;
                v_rowcount := v_sysrowcount;
            
                v_stmt = '
                -----------------------------------------
                /* Log total number of rows inserted in prc table into execution log */

                call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" 
                (
                  component := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type||'''
                , component_type := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""''
                , parent_component := '''||parent_component||'''
                , ultimate_parent_component := '''||ultimate_parent_component||'''
                , component_tree := ''''
                , regarding_object := '''||target_schema||'.'||target_table||'''
                , run_id := '||run_id||'
                , eventtype := ''prc\insert''
                , rowcount := '||v_rowcount||'
                , component_log_datetime := '''||now()::timestamp||'''
                , loglevel := 2
                , summary := ''job insert''
                , description := ''description''
                , execution_parameters := '''||v_params_json||'''
                );
                -----------------------------------------
                ';
            
                if debug_mode = true    
                    then raise notice '%', v_stmt;
                    else
                    -- Unfortunately we can't use the "execute v_stmt;" statement because a transaction within transaction is not allowed. So below statement is identical to above.
                    -- I've tried to avoid this by using a prc.proc_execution_log_entry function but then we can't use the command "execute" in combination with a variable. Also tried this with "perform" insead of "execute".
                    call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
                      component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type
                    , component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""'
                    , parent_component := parent_component
                    , ultimate_parent_component := ultimate_parent_component
                    , component_tree := ''
                    , regarding_object := target_schema||'.'||target_table
                    , run_id := run_id
                    , eventtype := 'prc\insert'
                    , rowcount := v_rowcount
                    , component_log_datetime := now()::timestamp
                    , loglevel := 2
                    , summary := 'job insert'
                    , description := ''
                    , execution_parameters := v_params_json 
                    );                  
                    commit;
                end if;
            
            end if;

else 
    if debug_mode = false   then
        raise notice 'insert into prc table skipped';
    end if;
end if;


-- When implicit_deletes is enabled set mf_deleted_ind if no value is provided
if process_type in ('HH','IU') and implicit_deletes = true 
    then
    v_stmt = '
    -----------------------------------------
    /* When implicit_deletes is enabled set mf_deleted_ind if no value is provided */
 
    update '||process_schema||'.'||target_schema||'_'||target_table||' 
        set mf_deleted_ind = false 
        where mf_deleted_ind is null;
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;

end if;

-- implicite deletes adds keys from target to processing for which no key is present in processing, (prc_row_status = 1  insert_delete for HH, 2 update for IU )
if process_type in ('HH','IU') and implicit_deletes = true 
    then
    if process_type = 'IU' then v_prc_row_status_del := 2;
       else v_prc_row_status_del := 1;
    end if;
    v_sql_select_tgt_del:= (
        select
        E'select \n'||
        ltrim(
            string_agg(text(
        case
          when c_tgt.column_name = 'mf_insert_datetime'         then ', now()::timestamp as '||'"'||c_tgt.column_name||'"'
          when c_tgt.column_name = 'mf_update_datetime'         then ', null::timestamp as '||'"'||c_tgt.column_name||'"'
          when c_tgt.column_name = 'mf_row_hash'                then ', null::uuid as mf_row_hash'
          when c_tgt.column_name = 'mf_run_id'                  then ', '||run_id||' as "'||c_tgt.column_name||'"'
          when c_tgt.column_name = 'mf_dp_available_datetime'   then ', '''||run_start_datetime||''''||'::timestamp as '||'"'||c_tgt.column_name||'"'
          when c_tgt.column_name = 'mf_dp_changed_datetime'     then ', null::timestamp as '||'"'||c_tgt.column_name||'"'
          when c_tgt.column_name = 'mf_deleted_ind'             then ', true as '||'"'||c_tgt.column_name||'"'
          when c_tgt.column_name = 'mf_dp_latest_int'           then ', null::bool as '||'"'||c_tgt.column_name||'"'
          --use columns from source when available
          when c_src.column_name = c_tgt.column_name            then ', "'||c_tgt.column_name||'"'
          else ', null::'||c_tgt.data_type||' as ' ||'"'||c_tgt.column_name||'"'
        end
        ), E'\n') 
        ||
        E'\n, '||v_prc_row_status_del||E' as prc_row_status'
        , ',')
        from information_schema.columns         as c_tgt
        left join information_schema.columns    as c_src on (   c_src.table_schema = source_schema 
                                                                and c_src.table_name = source_table 
                                                                and c_src.column_name = c_tgt.column_name )
        where c_tgt.table_schema = target_schema
        and c_tgt.table_name = target_table
        and c_tgt.column_name not like 'S2_%'
        group by 
          c_tgt.table_schema
        , c_tgt.table_name
    );
    --raise notice 'v_sql_select_tgt_del: %',v_sql_select_tgt_del;
    
    if  process_type = ('HH')
        then v_sql_where_tgt_del := '
            and mf_dp_latest_ind = true 
            and mf_deleted_ind = false';
    else v_sql_where_tgt_del := ' and mf_deleted_ind = false';
    end if;
    --raise notice 'v_sql_where_tgt_del: %',v_sql_where_tgt_del;
    
    v_stmt = '
    -----------------------------------------    
    /* Implicite deletes adds keys from target to processing for which no key is present in processing, (prc_row_status = 1  insert_delete for HH, 2 update for IU) */
    
    with tgt as (
        '||v_sql_select_tgt_del||'
        from '||target_schema||'.'||target_table||'
        where 1=1 '||v_sql_where_tgt_del||'
    ), src_keys as (
        select distinct 
        '||v_sql_bk_column||' 
        from '||process_schema||'.'||target_schema||'_'||target_table||'
    ) 
    
    insert into '||process_schema||'.'||target_schema||'_'||target_table ||E' (\n '
        ||v_sql_insert_columns_prc||
    E')
    select \n '
    ||v_sql_insert_columns_prc||'
    from tgt 
    left join src_keys using ('||v_sql_bk_column||') 
    where src_keys.'||v_sql_bk_column||' is null;
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;
    
        --log deleted rows into execution log 
        if logfromlevel <=  2
            then
            GET DIAGNOSTICS v_sysrowcount :=  ROW_COUNT;
            v_rowcount = v_sysrowcount;
            v_stmt = '
            -----------------------------------------  
            call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
              component := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type||'''
            , component_type := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""''
            , parent_component := '''||parent_component||'''
            , ultimate_parent_component := '''||ultimate_parent_component||'''
            , component_tree := ''''
            , regarding_object := '''||target_schema||'.'||target_table||'''
            , run_id := '||run_id||'
            , eventtype := ''prc\delete''
            , rowcount := '||v_rowcount||'
            , component_log_datetime := '''||now()::timestamp||'''
            , loglevel := 2
            , summary := ''''
            , description := ''''
            , execution_parameters := '''||v_params_json||'''
            );
            -----------------------------------------
            ';
        
            if debug_mode = true    
                then raise notice '%', v_stmt;
                else
                    -- Unfortunately we can't use the "execute v_stmt;" statement because a transaction within transaction is not allowed. So below statement is identical to above.
                    -- I've tried to avoid this by using a prc.proc_execution_log_entry function but then we can't use the command "execute" in combination with a variable. Also tried this with "perform" insead of "execute".
                    call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
                      component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type
                    , component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""'
                    , parent_component := parent_component
                    , ultimate_parent_component := ultimate_parent_component
                    , component_tree := ''
                    , regarding_object := target_schema||'.'||target_table
                    , run_id := run_id
                    , eventtype := 'prc\delete'
                    , rowcount := v_rowcount
                    , component_log_datetime := now()::timestamp
                    , loglevel := 2
                    , summary := ''
                    , description := ''
                    , execution_parameters := v_params_json 
                    );                
            end if;
        
        end if;
    commit;
    
end if;

--index on processing
if  process_type in ('HH','IU')
    then
    v_stmt = '
    -----------------------------------------  
    /* Create index on bk column */

    create index if not exists idx_""" + f"""{_PROCESS_SCHEMA_NAME}""" + r"""_'||v_sql_bk_column||' on '||process_schema||'.'||target_schema||'_'||target_table ||' ('
    ||v_sql_bk_column||
    ');
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
        raise notice 'Index created on """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""" table'; 
    end if;

    commit;

end if;

-- Hash calculation 
if  process_type in ('HH','IU')
    then
        v_sql_calc_hash := (
            select  
            'MD5(row('||string_agg(text('"' ||c_tgt.column_name|| '"::text'), ',' )||')::text)::uuid' as row_hash
            from information_schema.columns c_tgt
            where c_tgt.table_schema = target_schema
            and c_tgt.table_name = target_table
            --mf_fields except deleted ind are excluded from hash
            and ( c_tgt.column_name not like 'mf_%' or c_tgt.column_name = 'mf_deleted_ind' )
            and c_tgt.column_name <> 's1_'||target_table
            and c_tgt.column_name <> 's2_'||target_table
            group by c_tgt.table_schema,c_tgt.table_name
        );
        --raise notice 'hash calculation: %',v_sql_calc_hash;
    
        v_stmt = '
        -----------------------------------------  
        -- Set hash value in prc, only when not already supplied
        update '|| process_schema||'.'||target_schema||'_'||target_table||' 
            set mf_row_hash = '||v_sql_calc_hash||' 
            where mf_row_hash is null;
        -----------------------------------------
        ';
    
        if debug_mode = true    
            then raise notice '%', v_stmt;
            else execute v_stmt;    -- Use execute to execute plain sql
            raise notice 'hash update succeeded';  
        end if;
    
end if;

-- Row status hh records
if  process_type in ('HH')
    then
    
        v_stmt = '
        -----------------------------------------    
        /* Discard rows that already exist in target (set prc_row_status = -1 , discard) */

        update '||process_schema||'.'||target_schema||'_'||target_table||' as prc 
            set prc_row_status = -1 
            from '||target_schema||'.'||target_table||' as tgt 
            where tgt.'||v_sql_bk_column||' = """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".'||v_sql_bk_column||' 
            and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".mf_dp_available_datetime = tgt.mf_dp_available_datetime 
            and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".mf_row_hash = tgt.mf_row_hash 
            and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".prc_row_status <> -1;
        -----------------------------------------
        ';
    
        if debug_mode = true    
            then raise notice '%', v_stmt;
            else execute v_stmt;    -- Use execute to execute plain sql
        end if;

        GET DIAGNOSTICS v_sysrowcount := ROW_COUNT;
        v_rowcount = v_sysrowcount;

        v_stmt = '
        -----------------------------------------        
        /* Discard rows that follow up each other and are equal to previous version in the set (following up a version, having same hash,  set prc_row_status = -1 , discard) */
        update '||process_schema||'.'||target_schema||'_'||target_table ||' as prc 
            set prc_row_status = -1 
            from ( 
                select distinct 
                  '||v_sql_bk_column||'
                , mf_dp_available_datetime
                , mf_row_hash
                , case 
                    when lag(mf_row_hash) over (partition by '||v_sql_bk_column||' order by mf_dp_available_datetime) = mf_row_hash then true 
                    else false 
                  end   as duplicate
                from '||process_schema||'.'||target_schema||'_'||target_table||'
            ) as dedup 
            where dedup.'||v_sql_bk_column||' = """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".'||v_sql_bk_column||' 
            and dedup.mf_dp_available_datetime = """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".mf_dp_available_datetime 
            and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".prc_row_status <> -1
            and dedup.duplicate = true;
        -----------------------------------------
        ';
    
        if debug_mode = true    
            then raise notice '%', v_stmt;
            else execute v_stmt;    -- Use execute to execute plain sql
        end if;
    
        GET DIAGNOSTICS v_sysrowcount := ROW_COUNT;
        v_rowcount = v_sysrowcount;

        v_stmt = '
        -----------------------------------------    
        /* Discard rows that are equal to the latest version  (start later than  latest version, having same hash,  set prc_row_status = -1 , discard) */
        update '|| process_schema||'.'||target_schema||'_'||target_table ||' as """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""" 
            set prc_row_status = -1 
            from ( 
                select distinct 
                  '||v_sql_bk_column||'
                , first_value (mf_dp_available_datetime) over(partition by '||v_sql_bk_column||' order by mf_dp_available_datetime desc, mf_insert_datetime desc) mf_dp_available_datetime_latest 
                , first_value (mf_row_hash)              over(partition by '||v_sql_bk_column||' order by mf_dp_available_datetime desc, mf_insert_datetime desc) mf_row_hash_latest 
                from '||target_schema||'.'||target_table||'
            ) as tgt 
            where tgt.'||v_sql_bk_column||' = """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".'||v_sql_bk_column||' 
            and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".mf_dp_available_datetime > tgt.mf_dp_available_datetime_latest 
            and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".mf_row_hash = tgt.mf_row_hash_latest 
            and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".prc_row_status <> -1;
        -----------------------------------------
        ';
    
        if debug_mode = true    
            then raise notice '%', v_stmt;
            else execute v_stmt;    -- Use execute to execute plain sql
        end if;

    GET DIAGNOSTICS v_sysrowcount :=  ROW_COUNT;
    v_rowcount = v_rowcount + v_sysrowcount;

    -- Log discarded rows into execution log 
    if logfromlevel <=  2
        then
        v_stmt = '
        -----------------------------------------  
        """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
          component := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type||'''
        , component_type := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""''
        , parent_component := '''||parent_component||'''
        , ultimate_parent_component := '''||ultimate_parent_component||'''
        , component_tree := ''''
        , regarding_object := '''||target_schema||'.'||target_table||'''
        , run_id := '||run_id||'
        , eventtype := ''prc\discard''
        , rowcount := '||v_rowcount||'
        , component_log_datetime := '''||now()::timestamp||'''
        , loglevel := 2
        , summary := ''''
        , description := ''''
        , execution_parameters := '''||v_params_json||'''
        );
        -----------------------------------------
        ';
    
        if debug_mode = true    
            then raise notice '%', v_stmt;
            else
                -- Unfortunately we can't use the "execute v_stmt;" statement because a transaction within transaction is not allowed. So below statement is identical to above.
                -- I've tried to avoid this by using a prc.proc_execution_log_entry function but then we can't use the command "execute" in combination with a variable. Also tried this with "perform" insead of "execute".
                call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
                  component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type
                , component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""'
                , parent_component := parent_component
                , ultimate_parent_component := ultimate_parent_component
                , component_tree := ''
                , regarding_object := target_schema||'.'||target_table
                , run_id := run_id
                , eventtype := 'prc\discard'
                , rowcount := v_rowcount
                , component_log_datetime := now()::timestamp
                , loglevel := 2
                , summary := ''
                , description := ''
                , execution_parameters := v_params_json 
                );             
        end if;
        
    end if;
            
    commit;

end if;


-- Row status IU
if  process_type in ('IU')
    then
    v_stmt = '
    -----------------------------------------      
    /* Update row status for unchanged rows already existing in the target */

    update '||process_schema||'.'||target_schema||'_'||target_table||' prc 
        set prc_row_status = -1 
        from '||target_schema||'.'||target_table||' tgt 
        where 1=1 
        and prc.'||v_sql_bk_column||' = tgt.'||v_sql_bk_column||' 
        and prc.prc_row_status = 0 
        and prc.mf_row_hash = tgt.mf_row_hash;
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;
    
    GET DIAGNOSTICS v_sysrowcount :=  ROW_COUNT;
    v_rowcount = v_sysrowcount;

    -- Log discarded rows into execution log 
    if logfromlevel <=  2
        then        
        v_stmt = '
        -----------------------------------------  
        call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" 
        (
          component := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type||'''
        , component_type := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""''
        , parent_component := '''||parent_component||'''
        , ultimate_parent_component := '''||ultimate_parent_component||'''
        , component_tree := ''''
        , regarding_object := '''||target_schema||'.'||target_table||'''
        , run_id := '||run_id||'
        , eventtype := ''prc\discard''
        , rowcount := '||v_rowcount||'
        , component_log_datetime := '''||now()::timestamp||'''
        , loglevel := 2
        , summary := ''''
        , description := ''''
        , execution_parameters := '''||v_params_json||'''
        );
        -----------------------------------------
        ';
    
        if debug_mode = true    
            then raise notice '%', v_stmt;
            else
                -- Unfortunately we can't use the "execute v_stmt;" statement because a transaction within transaction is not allowed. So below statement is identical to above.
                -- I've tried to avoid this by using a prc.proc_execution_log_entry function but then we can't use the command "execute" in combination with a variable. Also tried this with "perform" insead of "execute".
                call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
                  component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type
                , component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""'
                , parent_component := parent_component
                , ultimate_parent_component := ultimate_parent_component
                , component_tree := ''
                , regarding_object := target_schema||'.'||target_table
                , run_id := run_id
                , eventtype := 'prc\discard'
                , rowcount := v_rowcount
                , component_log_datetime := now()::timestamp
                , loglevel := 2
                , summary := ''
                , description := ''
                , execution_parameters := v_params_json 
                );            
        end if;        

    end if;                         

    v_stmt = '
    ----------------------------------------- 
    /* Update row status for rows already existing in the target */

    update '||process_schema||'.'||target_schema||'_'||target_table||' prc 
        set 
          prc_row_status = 2
        , mf_run_id = '||run_id||' 
        from '||target_schema||'.'||target_table||' tgt 
        where 1=1 
        and prc.'||v_sql_bk_column||' = tgt.'||v_sql_bk_column||' 
        and prc.prc_row_status in (0, 1)
        and prc.mf_row_hash <> tgt.mf_row_hash;
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;

end if;



--set s1 key to value present in target table if a row already exists in the target or retrieve a key from sequence
if process_type in ('IU','HH') and upper(target_schema) = 'INT'
    then
    
    v_stmt = '
    -----------------------------------------     
    with tgt_surrogate_keys as (
        select distinct '
            ||v_sql_bk_column||
        ', '||v_sql_s1_column||' 
        from '||target_schema||'.'||target_table||'
    )

    update '||process_schema||'.'||target_schema||'_'||target_table||' """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""" 
        set '||v_sql_s1_column||' = tgt.'||v_sql_s1_column||' 
        from tgt_surrogate_keys tgt
        where """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".'||v_sql_bk_column||' = tgt.'||v_sql_bk_column||' 
        and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".prc_row_status in (0, 1, 2);
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;    
    
    v_stmt = '
    -----------------------------------------
    update '||process_schema||'.'||target_schema||'_'||target_table||' """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""" 
        set '||v_sql_s1_column||' = seq_s1.seq_s1_waarde
        from (
            select '
              ||v_sql_bk_column||'
            , nextval('||''''||'int.seq_'||v_sql_s1_column||''''||') seq_s1_waarde
            from (
                select distinct '
                ||v_sql_bk_column||' 
                from '||process_schema||'.'||target_schema||'_'||target_table||' 
                where '||v_sql_s1_column||' is null 
                and prc_row_status in (0, 1)
            ) unieke_bk
        ) seq_s1
        where 1=1 
        and """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".'||v_sql_bk_column||' = seq_s1.'|| v_sql_bk_column||';
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;      
    
end if;


-- Retrieve a s1_ key from sequence for TI
if process_type in ('TI') and upper(target_schema) = 'INT' and v_sql_s1_column is not null 
    then
    
    v_stmt = '
    -----------------------------------------
    /* Retrieve a s1_ key from sequence for TI */

    update '||process_schema||'.'||target_schema||'_'||target_table||' prc 
        set '||v_sql_s1_column||' = nextval('||''''||'int.seq_'||v_sql_s1_column||''''||') 
        where '||v_sql_s1_column||' is null 
        and prc_row_status in (0, 1);
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;  

end if;

-- Drop spatial index(es)
if process_type in ('HH','IU') and rebuild_spatial_index = true
    then    
    
    -- Generate sql to create spatial indexes 
    v_sql_drop_spatial_index := (
        select 
        string_agg(
        distinct 
            ' drop index if exists '||table_schema||'.'||table_name||'_sidx_geo_'||column_name||';','' )
        from information_schema.columns c_tgt
        where c_tgt.table_schema = target_schema
        and c_tgt.table_name = target_table
        and c_tgt.udt_name = 'geometry'
    );
    
    --raise notice 'v_sql_drop_spatial_index: %',v_sql_drop_spatial_index;
    if v_sql_drop_spatial_index is not null
        then
        v_stmt = v_sql_drop_spatial_index;
        
        if debug_mode = true    
            then raise notice '%', v_stmt;
            else execute v_stmt;    -- Use execute to execute plain sql
        end if;  

    end if;
end if;

commit;

if  process_type in ('HH','IU','TI') 
    then
    --create set of columns to insert in target
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
    --raise notice 'insert columns tgt: %',v_sql_insert_columns_tgt;    

    --create select on processing   including override for insert and update datetime only add rows with prc_row_status equal to 0 and 1            
    v_sql_select_prc := (   
        select 
        ' select '||
             string_agg(text(
          case
            when c_tgt.column_name = 'mf_deleted_ind'       then 'coalesce('||'"'||c_tgt.column_name||'"'||', false) as '||'"'||c_tgt.column_name||'"'
            when c_tgt.column_name = 'mf_insert_datetime'   then 'now()::timestamp as '||'"'||c_tgt.column_name||'"'
            when c_tgt.column_name = 'mf_update_datetime'   then 'now()::timestamp as '||'"'||c_tgt.column_name||'"'
            when c_tgt.column_name = 'mf_run_id'            then run_id ||' as "'||c_tgt.column_name||'"'
            else '"'||c_tgt.column_name||'"'
          end
        ), ',' )||' 
        from '||process_schema||'.'||target_schema||'_'||target_table||' 
        where prc_row_status in (0, 1);'
        from information_schema.columns as c_tgt
        where c_tgt.table_schema = target_schema
        and c_tgt.table_name = target_table
        and c_tgt.column_name <> 's2_'||c_tgt.table_name
        group by c_tgt.table_schema,c_tgt.table_name
    );   
    --raise notice 'select on prc: %',v_sql_select_prc;

    v_stmt = '
    -----------------------------------------
    /* Insert source table rows into target table */

    insert into '||target_schema||'.'||target_table||
    v_sql_insert_columns_tgt||
    v_sql_select_prc||'
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;  

    -- Log inserted tgt rows into execution log 
    if logfromlevel <=  2
        then
        GET DIAGNOSTICS v_sysrowcount := ROW_COUNT;
        v_rowcount := v_sysrowcount;
    
        v_stmt = '
        -----------------------------------------  
        call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
          component := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type||'''
        , component_type := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""''
        , parent_component := '''||parent_component||'''
        , ultimate_parent_component := '''||ultimate_parent_component||'''
        , component_tree := ''''
        , regarding_object := '''||target_schema||'.'||target_table||'''
        , run_id := '||run_id||'
        , eventtype := ''tgt\insert''
        , rowcount := '||v_rowcount||'
        , component_log_datetime := '''||now()::timestamp||'''
        , loglevel := 2
        , summary := ''''
        , description := ''''
        , execution_parameters := '''||v_params_json||'''
        );
        -----------------------------------------
        ';
    
        if debug_mode = true
            then raise notice '%', v_stmt;
            else
                -- Unfortunately we can't use the "execute v_stmt;" statement because a transaction within transaction is not allowed. So below statement is identical to above.
                -- I've tried to avoid this by using a prc.proc_execution_log_entry function but then we can't use the command "execute" in combination with a variable. Also tried this with "perform" insead of "execute".
                call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
                  component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type
                , component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""'
                , parent_component := parent_component
                , ultimate_parent_component := ultimate_parent_component
                , component_tree := ''
                , regarding_object := target_schema||'.'||target_table
                , run_id := run_id
                , eventtype := 'tgt\insert'
                , rowcount := v_rowcount
                , component_log_datetime := now()::timestamp
                , loglevel := 2
                , summary := ''
                , description := ''
                , execution_parameters := v_params_json 
                );
        end if;
    
    end if;

end if;



if  process_type in ('IU')
    then   
    v_sql_update_tgt :=  (
        select  
        ' update '|| target_schema||'.'||target_table ||' tgt set '|| 
         string_agg(
                 '"'||c_tgt.column_name||'"'||' = src.'||'"'||c_tgt.column_name||'"' 
                    , ',' ) ||
         ', mf_update_datetime = now()::timestamp 
        from '||process_schema||'.'||target_schema||'_'||target_table||' src 
        where src.prc_row_status in (2) 
        and src.'||v_sql_bk_column||' = tgt.'||v_sql_bk_column||';'
         from information_schema.columns as c_tgt
         where c_tgt.table_schema = target_schema
         and c_tgt.table_name = target_table
         and c_tgt.column_name not in (
           'mf_insert_datetime'
         , 'mf_update_datetime'
         , 's1_'||target_table
         , v_sql_bk_column
         )
         group by
           c_tgt.table_schema
         , c_tgt.table_name
    );      
    --raise notice 'v_sql_update_tgt: %',v_sql_update_tgt;          

    v_stmt = '
    -----------------------------------------
    /* Update rows in target table */
    
    '||v_sql_update_tgt||';
    -----------------------------------------
    ';

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;
    
    if logfromlevel <=  2
        then
        GET DIAGNOSTICS v_sysrowcount := ROW_COUNT;
        v_rowcount := v_sysrowcount;
    
        v_stmt = '
        -----------------------------------------
        call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
          component := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type||'''
        , component_type := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""''
        , parent_component := '''||parent_component||'''
        , ultimate_parent_component := '''||ultimate_parent_component||'''
        , component_tree := ''''
        , regarding_object := '''||target_schema||'.'||target_table||'''
        , run_id := '||run_id||'
        , eventtype := ''tgt\update''
        , rowcount := '||v_rowcount||'
        , component_log_datetime := '''||now()::timestamp||'''
        , loglevel := 2
        , summary := ''''
        , description := ''''
        , execution_parameters := '''||v_params_json||'''
        );
        -----------------------------------------
        ';
    
        if debug_mode = true
            then raise notice '%', v_stmt;
            else
                -- Unfortunately we can't use the "execute v_stmt;" statement because a transaction within transaction is not allowed. So below statement is identical to above.
                -- I've tried to avoid this by using a prc.proc_execution_log_entry function but then we can't use the command "execute" in combination with a variable. Also tried this with "perform" insead of "execute".
                call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
                  component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type
                , component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""'
                , parent_component := parent_component
                , ultimate_parent_component := ultimate_parent_component
                , component_tree := ''
                , regarding_object := target_schema||'.'||target_table
                , run_id := run_id
                , eventtype := 'tgt\update'
                , rowcount := v_rowcount
                , component_log_datetime := now()::timestamp
                , loglevel := 2
                , summary := ''
                , description := ''
                , execution_parameters := v_params_json 
                );
        end if;

    end if;

end if;

--set/update  mf_dp_changed_datetime and mf_dp_latest_ind in target for processed keys
if  process_type in ('HH')
    then

    v_stmt = '
    -----------------------------------------
    with
    tgt_values as (
        select '
          ||v_sql_bk_column||'
        , mf_dp_available_datetime
        , mf_insert_datetime
        , ctid as rowid
        , coalesce(lead(mf_dp_available_datetime) over (
            partition by '
            ||v_sql_bk_column||' 
            order by 
              mf_dp_available_datetime
            , mf_insert_datetime
            , ctid
          ), ''9999-12-31''::timestamp)         as mf_dp_changed_datetime
        , case 
            when lead(mf_dp_available_datetime) over (
                partition by '
                    ||v_sql_bk_column||'
                order by 
                  mf_dp_available_datetime
                , mf_insert_datetime
                , ctid
            ) is null then true
            else false 
          end as mf_dp_latest_ind
        from '||target_schema||'.'||target_table||'
    ), prc_keys as (
        select distinct '
          ||v_sql_bk_column||' 
        from '||process_schema||'.'||target_schema||'_'||target_table||' 
        where prc_row_status <> -1
    )

    update '||target_schema||'.'||target_table||' as tgt
        set
          mf_dp_changed_datetime = tgt_values.mf_dp_changed_datetime
        , mf_dp_latest_ind = tgt_values.mf_dp_latest_ind
        , mf_update_datetime = now()::timestamp
        from tgt_values
        inner join prc_keys using ( '||v_sql_bk_column||')
        where 1=1
        and tgt_values.rowid= tgt.ctid
        and (       tgt_values.mf_dp_changed_datetime <> tgt.mf_dp_changed_datetime
                or  tgt.mf_dp_changed_datetime is null
        );
    -----------------------------------------
    ';

    if debug_mode = true
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;  

    commit;

end if;


-- Create spatial index(es)
if process_type in ('HH','IU','TI') and rebuild_spatial_index = true
    then    
    --generate sql to create spatial indexes
    v_sql_spatial_index := (
        -----------------------------------------
        /* Create spatial index(es) */
    
        select 
        string_agg(
        'create index if not exists '||table_name||'_sidx_geo_'||column_name||' on '||table_schema||'.'||table_name||' using gist ('||column_name||')',';' ) 
        from information_schema.columns as c_tgt
        where c_tgt.table_schema = target_schema
        and c_tgt.table_name = target_table
        and c_tgt.udt_name = 'geometry'
        group by 
          table_schema
        , table_name
        -----------------------------------------
    );
    --raise notice 'v_sql_spatial_index: %',v_sql_spatial_index;
    
    if  v_sql_spatial_index is null
    then
        v_stmt = '
        -----------------------------------------
        call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
          component := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type||'''
        , component_type := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""''
        , parent_component := '''||parent_component||'''
        , ultimate_parent_component := '''||ultimate_parent_component||'''
        , component_tree := ''''
        , regarding_object := '''||target_schema||'.'||target_table||'''
        , run_id := '||run_id||'
        , eventtype := ''warning''
        , rowcount := -1
        , component_log_datetime := '''||now()::timestamp||'''
        , loglevel := 3
        , summary := ''spatial index not (re)built''
        , description := ''no geo column found, spatial index not (re)built''
        , execution_parameters := '''||v_params_json||'''
        );
        -----------------------------------------
        ';

        if debug_mode = true
            then raise notice '%', v_stmt;
            else
                -- Unfortunately we can't use the "execute v_stmt;" statement because a transaction within transaction is not allowed. So below statement is identical to above.
                -- I've tried to avoid this by using a prc.proc_execution_log_entry function but then we can't use the command "execute" in combination with a variable. Also tried this with "perform" insead of "execute".
                call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
                  component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type
                , component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""'
                , parent_component := parent_component
                , ultimate_parent_component := ultimate_parent_component
                , component_tree := ''
                , regarding_object := target_schema||'.'||target_table
                , run_id := run_id
                , eventtype := 'warning'
                , rowcount := -1
                , component_log_datetime := now()::timestamp
                , loglevel := 3
                , summary := 'spatial index not (re)built'
                , description := 'no geo column found, spatial index not (re)built'
                , execution_parameters := v_params_json 
                );
        end if;

     else
        v_stmt = v_sql_spatial_index;
        
        if debug_mode = true
            then raise notice '%', v_stmt;
            else execute v_stmt;    -- Use execute to execute plain sql
        end if;
    
     end if;
end if;



if  process_type in ('SC')
    then
    v_sql_tmp_tgt_key_hash := ('
        drop table if exists tmp_tgt_key_hash; 
        
        create temp table tmp_tgt_key_hash as 
            select '
              ||v_sql_pk_column||'
            , md5(tgt.*::text) as rij_hash
            from '||target_schema||'.'||target_table||' tgt; 
        
        alter table tmp_tgt_key_hash 
            add primary key ('||v_sql_pk_column||');
    ');
    --raise notice 'v_sql_tmp_tgt_key_hash: %',v_sql_tmp_tgt_key_hash;
    v_stmt = v_sql_tmp_tgt_key_hash;

    if debug_mode = true
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;


    v_sql_tmp_src_key_hash := ('
        drop table if exists tmp_src_key_hash;
        
        create temp table tmp_src_key_hash as 
            select '
              ||v_sql_pk_column||'
            , md5(src.*::text) as rij_hash 
            from '||source_schema||'.'||source_table||' src;

        alter table tmp_src_key_hash 
            add primary key ('||v_sql_pk_column||');
    ');
    --raise notice 'v_sql_tmp_src_key_hash: %',v_sql_tmp_src_key_hash;
    v_stmt = v_sql_tmp_src_key_hash;

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;   


    v_sql_tmp_src_tgt_action := ('
        drop table if exists tmp_src_tgt_action; 

        create temp table tmp_src_tgt_action as 
            select 
              coalesce(src.'||v_sql_pk_column||', tgt.'||v_sql_pk_column||') '
              ||v_sql_pk_column||'
            , case 
                when src.'||v_sql_pk_column||' is null then '||'''delete'''||' 
                when tgt.'||v_sql_pk_column||' is null then '||'''insert'''||' 
                when src.rij_hash = tgt.rij_hash then '||'''discard'''||' 
                when src.rij_hash <> tgt.rij_hash then '||'''update'''||'  
              end as db_action 
            from tmp_src_key_hash as src 
            full outer join tmp_tgt_key_hash as tgt using('||v_sql_pk_column||');
    ');
    --raise notice 'v_sql_tmp_src_tgt_action: %',v_sql_tmp_src_tgt_action;
    v_stmt = v_sql_tmp_src_tgt_action;

    if debug_mode = true
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;


    -- Insert new records in target
    v_sql_insert_tgt_sc := (
        select ' 
        insert into '|| target_schema||'.'||target_table ||'('||
        string_agg( '"'||c_src.column_name||'"',',')||') select '||
        string_agg( '"'||c_src.column_name||'"',',')||' 
        from '||'pte'||'.'||source_table||' 
        inner join tmp_src_tgt_action using ('||v_sql_pk_column||')
        where db_action= ''insert''' 
        from information_schema.columns c_src
        where table_schema = source_schema 
        and table_name= source_table
    );
    --raise notice 'v_sql_insert_tgt_sc: %',v_sql_insert_tgt_sc;
    v_stmt = v_sql_insert_tgt_sc;

    if debug_mode = true    
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;


    -- Update records
    v_sql_update_tgt_sc :=  ( 
        select ' 
        update '||target_schema||'.'||target_table||' as tgt 
            set '|| 
            string_agg('"'||c_tgt.column_name||'"'||' = src.'||'"'||c_tgt.column_name||'"' , ',' ) ||' 
            from '|| source_schema||'.'||source_table||' src 
            where 1=1 
            and src.'||v_sql_pk_column||' = tgt.'||v_sql_pk_column||' 
            and tgt.'||v_sql_pk_column||' in (
                select '||v_sql_pk_column||' 
                from tmp_src_tgt_action 
                where db_action = '||'''update'''||'
            );'
        from information_schema.columns as c_tgt
        where c_tgt.table_schema = target_schema
        and c_tgt.table_name = target_table
        and c_tgt.column_name <> v_sql_pk_column
        group by 
          c_tgt.table_schema
        , c_tgt.table_name
    ); 
    --raise notice 'v_sql_update_tgt_sc: %',v_sql_update_tgt_sc;
    v_stmt = v_sql_update_tgt_sc;

    if debug_mode = true
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;


    -- Delete records with are no longer in source
    v_sql_delete_tgt_sc := ('
        delete from '||target_schema||'.'||target_table|| ' 
        where 1=1 
        and '||v_sql_pk_column||' in (
            select '
              ||v_sql_pk_column||' 
            from tmp_src_tgt_action 
            where db_action = '||'''delete'''||
        ');'
    );
    --raise notice 'v_sql_delete_tgt_sc: %',v_sql_delete_tgt_sc;
    v_stmt = v_sql_delete_tgt_sc;

    if debug_mode = true
        then raise notice '%', v_stmt;
        else execute v_stmt;    -- Use execute to execute plain sql
    end if;

end if;



--log start into execution log 
if logfromlevel <=  2
    then

    v_stmt = '
    -----------------------------------------
    call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" 
    (
      component := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type||'''
    , component_type := ''""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""''
    , parent_component := '''||parent_component||'''
    , ultimate_parent_component := '''||ultimate_parent_component||'''
    , component_tree := ''''
    , regarding_object := '''||target_schema||'.'||target_table||'''
    , run_id := '||run_id||'
    , eventtype := ''finish''
    , rowcount := -1
    , component_log_datetime := '''||now()::timestamp||'''
    , loglevel := 2
    , summary := ''''
    , description := ''''
    , execution_parameters := '''||v_params_json||'''
    );
    -----------------------------------------
    ';

    if debug_mode = true
        then raise notice '%', v_stmt;
        else
            -- Unfortunately we can't use the "execute v_stmt;" statement because a transaction within transaction is not allowed. So below statement is identical to above.
            -- I've tried to avoid this by using a prc.proc_execution_log_entry function but then we can't use the command "execute" in combination with a variable. Also tried this with "perform" insead of "execute".
            call """ + f"""{_PROCESS_SCHEMA_NAME}""" + r""".""" + f"""{_LOG_FUNCTION_NAME}""" + r""" (
              component := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""_'||process_type
            , component_type := '""" + f"""{_PROCESS_FUNCTION_NAME}""" + r"""'
            , parent_component := parent_component
            , ultimate_parent_component := ultimate_parent_component
            , component_tree := ''
            , regarding_object := target_schema||'.'||target_table
            , run_id := run_id
            , eventtype := 'finish'
            , rowcount := -1
            , component_log_datetime := now()::timestamp
            , loglevel := 2
            , summary := ''
            , description := ''
            , execution_parameters := v_params_json 
            );
    end if;

end if;

if debug_mode = false    
    then raise notice 'finish';
end if; 

end ;
$procedure$

/* 
--voorbeeld aanroep
call prc.proc_pre_post_process (
, source_schema := 'pte'
, source_table := 'wgp_materieel'
, process_schema := 'prc'
, target_schema := 'pte'
, target_table := 'mdb_wgp_materieel'
, process_type := 'SC'
, implicit_deletes := true
, run_id := $run_id
, parent_component := 'stg_dpt_buurt.kjb'
, ultimate_parent_component := 'oj_verhardingen.kjb'
, logfromlevel := $logfromlevel
, skip_prc_prepare := true
, rebuild_spatial_index := false
, run_start_datetime := $run_start_datetime
, debug_mode = false
);
*/

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
