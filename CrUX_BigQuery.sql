with may_lcp_raw as (SELECT origin,
form_factor.name as form,
'May' as Month,
lcp.start as start,
lcp.end as ed,
lcp.density as density
FROM `chrome-ux-report.country_us.202305` a
cross join UNNEST(a.largest_contentful_paint.histogram.bin) AS lcp
where origin in ('https://www.sfgate.com', 'https://www.sfchronicle.com', 'https://www.chron.com', 'https://www.houstonchronicle.com', 'https://www.timesunion.com')
and effective_connection_type.name = '4G'),

may_lcp_flagged as (select origin, form, month, density,
case
  when ed <= 2500 then 'good'
  when ed between 2500 and 4000 then 'needs imp'
  when ed >= 4000 then 'poor'
end as perf_flag
from may_lcp_raw),

may_lcp as (select distinct origin, form, Month, perf_flag,
SUM(density) OVER(PARTITION BY origin, form, perf_flag) As sumdens
from may_lcp_flagged
order by origin, form, Month, perf_flag),

apr_lcp_raw as (SELECT origin,
form_factor.name as form,
'Apr' as Month,
lcp.start as start,
lcp.end as ed,
lcp.density as density
FROM `chrome-ux-report.country_us.202304` a
cross join UNNEST(a.largest_contentful_paint.histogram.bin) AS lcp
where origin in ('https://www.sfgate.com', 'https://www.sfchronicle.com', 'https://www.chron.com', 'https://www.houstonchronicle.com', 'https://www.timesunion.com')
and effective_connection_type.name = '4G'),

apr_lcp_flagged as (select origin, form, month, density,
case
  when ed <= 2500 then 'good'
  when ed between 2500 and 4000 then 'needs imp'
  when ed >= 4000 then 'poor'
end as perf_flag
from apr_lcp_raw),

apr_lcp as (select distinct origin, form, Month, perf_flag,
SUM(density) OVER(PARTITION BY origin, form, perf_flag) As sumdens
from apr_lcp_flagged
order by origin, form, Month, perf_flag),

mar_lcp_raw as (SELECT origin,
form_factor.name as form,
'March' as Month,
lcp.start as start,
lcp.end as ed,
lcp.density as density
FROM `chrome-ux-report.country_us.202303` a
cross join UNNEST(a.largest_contentful_paint.histogram.bin) AS lcp
where origin in ('https://www.sfgate.com', 'https://www.sfchronicle.com', 'https://www.chron.com', 'https://www.houstonchronicle.com', 'https://www.timesunion.com')
and effective_connection_type.name = '4G'),

mar_lcp_flagged as (select origin, form, month, density,
case
  when ed <= 2500 then 'good'
  when ed between 2500 and 4000 then 'needs imp'
  when ed >= 4000 then 'poor'
end as perf_flag
from mar_lcp_raw),

mar_lcp as (select distinct origin, form, Month, perf_flag,
SUM(density) OVER(PARTITION BY origin, form, perf_flag) As sumdens
from mar_lcp_flagged
order by origin, form, Month, perf_flag)

select * from mar_lcp
union all
select * from apr_lcp
union all
select * from may_lcp

