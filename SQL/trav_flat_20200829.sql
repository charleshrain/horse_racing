
create table flat as
SELECT
a.horseid,
a.datum,
a.bana,
a.lopp,
a.spar,
a.distans,
a.tillagg,
a.placering,
a.tid,
a.odds,
a.kusk,
a.verklspar,
b.startsatt,
b.division,
b.banforh,
b.v75 as avd75,
b.forstapris,
tvlid,
v1,
v2,
v3,
v4,
c.v5,
v6,
v7,
v8,
v9,
v10,
v11,
v12,
v13,
v14,
v15,
v16,
v17,
v18,
v19,
v20,
v21,
v22,
v23,
v24,
v25,
v26,
v27,
v28,
v29,
v30,
v31,
v32,
v33,
v34,
v35,
v36,
v37,
v38,
v39,
v40,
v41,
v42,
v43,
v44,
v45,
v46,
v47,
v48,
v49,
v50,
v51,
v52,
v53,
v54,
v55,
v56,
v57,
v58,
v59,
v60,
v61,
v62,
avdv75,
v63,
v64,
c.v65,
v66,
v67,
v68,
v69,
v70,
v71,
v72,
v73,
antstreck,
v74,
c.v75,
v76,
v77,
v78,
v79,
v80,
v81,
v82,
v83,
v84,
v85,
c.v86,
v87,
v88,
v89,
v90,
v91,
v92,
v93,
v94,
v95,
v96,
v97,
v98,
v99,
coalesce(sum(case when plac = '1' then 1 else 0 end) over (partition by a.horseid, extract(year from a.datum) order by a.datum rows between unbounded preceding and 1 preceding)::float4/
count(*) over (partition by a.horseid, extract(year from a.datum)  order by a.datum rows between unbounded preceding and 1 preceding),0) as win_perc_curr

FROM
     lopp a
        JOIN
    tvl b ON a.datum = b.datum AND a.bana = b.bana
        AND a.lopp = b.lopp
        JOIN
    prog c ON b.id = c.tvlid AND a.horseid = c.horseid;

-- ALTER TABLE flat
-- ADD COLUMN tidrnk integer;

-- with new_values as (
--    SELECT tvlid, horseid,
--           percent_rank() over (partition by tvlid order by v33 desc) as rank
--     from travels
--
-- )
-- update travels as tr
--   set delete = nv.rank
-- from new_values nv
-- where nv.tvlid = tr.tvlid and nv.horseid = tr.horseid;
--
-- --won
-- --pngrank
-- --intkrank
-- --tidrank
-- --kuskrank
-- end;
