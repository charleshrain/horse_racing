/* variables for analysis
RaceNo
   TrackNo
   Jockey
   BetPerc
   MoneyRank
   WinsPerc
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
create table flat as
SELECT
a.horseid,
a.datum,
a.bana,
a.lopp,
a.spar as TrackNo,
a.distans,
a.tillagg,
a.placering,
       CASE when a.placering = '1' then 1 else 0 end  as won,
a.tid,
a.odds as WinOdds,
--avg(CASE WHEN odds~E'^\\d+$' THEN odds::float4 ELSE NULL END ) over (partition by a.horseid order by a.datum rows between unbounded preceding and 1 preceding) as AvgOdds,
a.kusk as Jockey,
a.verklspar,
b.startsatt,
b.division,
b.banforh,
b.v75 as avd75,
b.forstapris,
tvlid,
v1 as ownerWinPerc, --agare segerP,
case when v10 = 0 then 0 else v10::decimal/max(v10::decimal) over (partition by tvlid) end  as MoneyRank, --prissumma totalt v10
v25, --odds
v33 as WinsPerc, --segerP horse
v39 as rank_streck, --ranking streck
v40 as BetPerc,
v41 as RekordTid, --basta tid
v43 as tillaggMetre, --tillagg meter
v44 as trainerWinPerc, --segerP tranare
v47 as v75WinPerc, --v75 segerP
v51 as PlacePerc, --Platsprocent enligt inställning beräkningsdagar.
c.v65 as avdV65,
case when v67 = 0 then 0 else v67::decimal/max(v67::decimal) over (partition by tvlid) end  as PointsPerc,
v72 as jockeyRank, --kuskrank aktuall tavling
antstreck as BetPerc4,
v76 as BetPerc2, --antal streck
v77 as BetPerc3, --streckProcent

coalesce(sum(case when plac = '1' then 1 else 0 end) over (partition by a.horseid, extract(year from a.datum) order by a.datum rows between unbounded preceding and 1 preceding)::float4/
count(*) over (partition by a.horseid, extract(year from a.datum)  order by a.datum rows between unbounded preceding and 1 preceding),0) as WinPercCurrent,

coalesce(sum(case when plac IN ('1','2', '3') then 1 else 0 end) over (partition by a.horseid order by a.datum rows between unbounded preceding and 1 preceding)::float4/
         count(*) over (partition by a.horseid order by a.datum rows between unbounded preceding and 1 preceding),0) as PlaceP

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
