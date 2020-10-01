

create table lopp2 as
select * from (
                  select a.*,

                         coalesce(sum(case when a.placering = '1' then 1 else 0 end) over (partition by a.horseid, extract(year from a.datum) order by a.datum rows between unbounded preceding and 1 preceding)::float4/
                                  count(*) over (partition by a.horseid, extract(year from a.datum)  order by a.datum rows between unbounded preceding and 1 preceding),0) as WinPercCurrent,

                         coalesce(sum(case when a.placering IN ('1','2', '3') then 1 else 0 end) over (partition by a.horseid order by a.datum rows between unbounded preceding and 1 preceding)::float4/
                                  count(*) over (partition by a.horseid order by a.datum rows between unbounded preceding and 1 preceding),0) as PlaceP,
                         coalesce(sum(case when a.placering = '1' then 1 else 0 end) over (partition by a.horseid order by a.datum rows between unbounded preceding and 1 preceding)::float4/
                                  count(*) over (partition by a.horseid order by a.datum rows between unbounded preceding and 1 preceding),0) as WinsPerc,
                         case
                             when (tid similar to '\d\d\d\w+' or tid similar to '\d\d\d' or tid similar to '\d\d\' or
                                   tid similar to '\d\d\w+') and placering in ('1', '2', '3', '4', '5', '6', '7')
                                 then regexp_replace(tid, '[[:alpha:]]', '', 'g')::numeric
                             when NOT (tid similar to '\d\d\d\w+' or tid similar to '\d\d\d' or tid similar to '\d\d\' or
                                       tid similar to '\d\d\w+') and placering in ('1', '2', '3', '4', '5', '6', '7')
                             then NULL end as tidclean,
                  case when lower(distans) like 'k%' then 'K' when  lower(distans)  like 'l%' then 'L'
                      when lower(distans)  like 'm%' then 'M' when lower(distans)  like '%s' then 'S' else 'UNKNOWN' end as distans2



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
min(tidclean) over (partition by a.horseid, b.startsatt order by a.datum rows between unbounded preceding and 1 preceding) as mintid,
--a.kusk as Jockey,
a.verklspar,
case when a.spar between 1 and 7 then 'FS' else 'BS' end as framspar,
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


--create v75 table
create table v75flat as
    select * from flat where avd75 between 1 and 7;


