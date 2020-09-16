create table travflat as
SELECT 
    a.id,
    c.tvlid,
    b.loppid,
    a.datum,
    a.bana,
    c.horseid,
    a.distans,
    a.tillagg,
    a.placering,
    a.tid,
    a.odds,
    a.verklspar,
    b.startsatt,
    b.division,
    b.banforh,
	c.avdv75,
    b.forstapris,
    c.rank,
    c.v72 KuskRank,
    c.v67 atgpoang,
    c.v47 V75Proc,
    c.v41 TidBesta,
    c.v40 StreckP,
    c.v33 SegerP,
    c.v10 Intakt,
    CASE WHEN a.placering = 1 THEN 1 ELSE 0 END  vinnare
   
FROM
    trav2.lopp a
        JOIN
    trav2.tvl b ON a.datum = b.datum AND a.bana = b.bana
        AND a.lopp = b.lopp
        JOIN
    trav2.prog c ON b.id = c.tvlid AND a.horseid = c.horseid
WHERE b.v75 BETWEEN 1 and 7;
--
-- select * from trav2.travflat;
--
-- ALTER TABLE trvflt
-- ADD COLUMN tidrnk integer;
--
--
--
-- update trvflt t
--     set tidrnk = rnk
--     from (
--         select
--             tvlid,horseid,
--             rank() over (partition by tvlid order by "TidBesta" asc nulls last ) as rnk
--         from trvflt
--     ) s
--     where t.tvlid = s.tvlid and t.horseid = s.horseid;