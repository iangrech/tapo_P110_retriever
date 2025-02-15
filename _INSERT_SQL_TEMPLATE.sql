insert into {table_name} (sourcefile, ts, {value_column})
select
    drvd.s, drvd.t, drvd.v
from
    (select '{sourcefile}' s, '{ts}'::timestamp t, '{value}' v) drvd
        left outer join {table_name} tn
            on drvd.t = tn.ts
where
    tn.ts is null
    ;