/* variables for analysis
RaceNo
   TrackNo
   Jockey
   BetPerc --doesn't seem to have correct numbers
   MoneyRank
   WinsPerc - check calc with actual
   WinOdds
   RekordTid
   PointsPerc
   WinPercCurrent
   AvgOdds
   PlacePerc
   Trainer
   JockeyRank
   TrainerWinPerc


 */

create table lopp2 as
select * from (
                  select a.*,

                         coalesce(sum(case when a.placering = '1' then 1 else 0 end) over (partition by a.horseid, extract(year from a.datum) order by a.datum rows between unbounded preceding and 1 preceding)::float4/
                                  count(*) over (partition by a.horseid, extract(year from a.datum)  order by a.datum rows between unbounded preceding and 1 preceding),0) as WinPercCurrent,

                         coalesce(sum(case when a.placering IN ('1','2', '3') then 1 else 0 end) over (partition by a.horseid order by a.datum rows between unbounded preceding and 1 preceding)::float4/
                                  count(*) over (partition by a.horseid order by a.datum rows between unbounded preceding and 1 preceding),0) as PlaceP,
                         coalesce(sum(case when a.placering = '1' then 1 else 0 end) over (partition by a.horseid order by a.datum rows between unbounded preceding and 1 preceding)::float4/
                                  count(*) over (partition by a.horseid order by a.datum rows between unbounded preceding and 1 preceding),0) as WinsPerc

                  FROM lopp a) mytab;





create table flat as
SELECT
a.horseid,
a.datum,
a.bana,
--a.lopp,
a.spar as TrackNo,
a.distans,
a.tillagg,
a.placering,
       CASE when a.placering = '1' then 1 else 0 end  as won,
a.WinPercCurrent,
a.WinsPerc,
a.placep,
a.tid,
--a.kusk as Jockey,
a.verklspar,
case when trackno between 1 and 7 then 'FS' else 'BS' end as framspar,
b.startsatt,
b.division,
b.banforh,
b.v75 as avd75,
tvlid,
v1 as ownerWinPerc, --agare segerP,
case when v11 = 0 then 0 else v11::decimal/max(v11::decimal) over (partition by tvlid) end  as MoneyRank,

v39 as StreckRank, --ranking streck
v40 as BetPerc,
--v41 as RekordTid, --basta tid
v43 as tillaggMetre, --tillagg meter
v44 as trainerWinPerc, --segerP tranare
-- c.v65 as avdV65,
case when v67 = 0 then 0 else v67::decimal/max(v67::decimal) over (partition by tvlid) end  as PointsPerc,
v72 as jockeyRank --kuskrank aktuall tavling

FROM
     lopp2 a
        JOIN
    tvl b ON a.datum = b.datum AND a.bana = b.bana
        AND a.lopp = b.lopp
        JOIN
    prog c ON b.id = c.tvlid AND a.horseid = c.horseid;

create table v75flat as
    select * from flat where avd75 between 1 and 7;

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
