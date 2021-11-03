
INSERT INTO  dim_marketing_cost_per_conversion(num_conversions, end_date)
 select count(*), cast(application_date as timestamp) from spectrum.creditgenie_lead where application_date = '{{params.date_string}}'
                                        and environmentname = 'prod'
                                        group by application_date;

update dim_marketing_cost_per_conversion
set num_conversions = (select sum((select count(*) from spectrum.creditgenie_application where application_date = '{{params.date_string}}'
                       and environmentname = 'freetool') + (select sum(num_conversions) from dim_marketing_cost_per_conversion
                            where end_date = current_date - 1)))
where end_date = current_date - 1;

update
    dim_marketing_cost_per_conversion
    set total_spend = (select sum(spend) from facebook_ads_integration_sg.ads_insights where date_stop = current_date - 1)
where end_date = current_date - 1;

update dim_marketing_cost_per_conversion
set cost_per_conversion = num_conversions / total_spend 
where end_date = current_date - 1;
