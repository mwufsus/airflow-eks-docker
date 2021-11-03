drop table if exists staging.temp_apps;

select id,
       reasonfornotconnectingbank,
       environmentname,
       createdat,
       lastviewedpage,
       lastcompletedpage,
       kindofrelief,
       utmsource,
       utmcampaign,
       utmcontent,
       utmmedium,
       utmterm,
       router,
       convert_timezone('UTC', 'EST', createdat) as est_converted_date
into staging.temp_apps
from spectrum.creditgenie_application
where application_date = '{{params.date_string}}'
and environmentname not in ('staging','prod','master','main','freetool')
and id not in (select id from daily_page_conversions);


