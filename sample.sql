SELECT *
FROM (
        SELECT n.nspname,
            c.relname,
            a.attname,
            a.atttypid,
            a.attnotnull
            OR (
                t.typtype = 'd'
                AND t.typnotnull
            ) AS attnotnull,
            a.atttypmod,
            a.attlen,
            t.typtypmod,
            row_number() OVER (
                PARTITION BY a.attrelid
                ORDER BY a.attnum
            ) AS attnum,
            nullif(a.attidentity, '') as attidentity,
            pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,
            dsc.description,
            t.typbasetype,
            t.typtype
        FROM pg_catalog.pg_namespace n
            JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
            JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid)
            JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
            LEFT JOIN pg_catalog.pg_attrdef def ON (
                a.attrelid = def.adrelid
                AND a.attnum = def.adnum
            )
            LEFT JOIN pg_catalog.pg_description dsc ON (
                c.oid = dsc.objoid
                AND a.attnum = dsc.objsubid
            )
            LEFT JOIN pg_catalog.pg_class dc ON (
                dc.oid = dsc.classoid
                AND dc.relname = 'pg_class'
            )
            LEFT JOIN pg_catalog.pg_namespace dn ON (
                dc.relnamespace = dn.oid
                AND dn.nspname = 'pg_catalog'
            )
        WHERE c.relkind in ('r', 'p', 'v', 'f', 'm')
            and a.attnum > 0
            AND NOT a.attisdropped
            AND n.nspname LIKE 'public'
            AND c.relname LIKE 'review_rating'
    ) c
WHERE true
ORDER BY nspname,
    c.relname,
    attnum

SELECT * FROM review r, item i WHERE i.i_id = r.i_id and r.i_id=112 ORDER BY rating DESC, r.creation_date DESC LIMIT 10

UPDATE item SET title = 'U14%M~hkSY42=VWHHKSaO0MOw^rF$tc7[m,%Kot]P,^`=Z([$*<oFQnh@RbK$O{Vke+Ote]kvg0YpsSa""Mn74FHw p =s(H)c,xw422lL^2@^l.}u&Qi!Et1UUm@$V2 ' WHERE i_id=402