### Запрос

```sql
with pl_swt_agg_by_day as (
    select
        date_format(from_unixtime(kollectorts / 1000) + interval '3' hour, '%Y-%m-%d') as watch_date,
        coalesce(merged.current, pl_swt.userid) as user_uid,
        elementuid as element_uid,
        lower(devicetype) as device_type,
        lower(devicemodel) as device_model,
        upper(consumptionmode) as consumption_mode,
        sum(watched_time) as view_duration
    from pl_server_watched_time
    left join merged
         on merged.previous = pl_swt.userid
    where year * 10000 + month * 100 + day >= cast(date_format(current_date - interval '7' day, '%Y%m%d') as bigint)
					and year * 10000 + month * 100 + day < cast(date_format(current_date, '%Y%m%d') as bigint)
          and elementuid is not null and userid is not null
    group by 1, 2, 3, 4, 5, 6
)
select
	pl_p.start_timestamp,
	pl_p.user_uid,
	pl_p.element_uid,
	pl_p.element_type,
	pl_p.device_type,
	pl_p.device_manufacturer,
	pl_p.device_model,
	pl_p.audio_language,
	pl_p.subtitle_language
	ctx_trnsct.purchase_consumption_mode,
	coalesce(pl_p.consumption_mode, pl_swt_dev_model.consumption_mode, pl_swt_dev_type.consumption_mode) as consumption_mode,
	coalesce(pl_swt_dev_model.view_duration, pl_swt_dev_type.view_duration, 0) as view_duration
from (
        select distinct
            date_format(from_unixtime(kollectorts / 1000) + interval '3' hour, '%Y-%m-%d') as watch_date,
            coalesce(merged.current, pl_playback.userid) as user_uid,
            elementuid as element_uid,
            case
                when assettype = 'trailer' then 'trailer'
                else lower(elementtype)
            end as element_type,
            lower(devicetype) as device_type,
            lower(devicemanufacturer) as device_manufacturer,
            lower(devicemodel) as device_model,
            lower(first_value(consumptionmode) ignore nulls over (
                partition by coalesce(merged.current, pl_playback.userid), elementuid, lower(devicemodel)
                order by kollectorts desc
                rows between unbounded preceding and unbounded following
            )) as consumption_mode,
            lower(first_value(audiolanguage) ignore nulls over (
                partition by coalesce(merged.current, pl_playback.userid), elementuid, lower(devicemodel)
                order by kollectorts desc
                rows between unbounded preceding and unbounded following
            )) as audio_language,
            first_value(kollectorts / 1000) ignore nulls over (
                partition by coalesce(merged.current, pl_playback.userid), elementuid, lower(devicemodel) order by kollectorts rows between unbounded preceding and unbounded following
            ) as start_timestamp
        from pl_playback
        left join merged
             on merged.previous = pl_playback.userid
        where year * 10000 + month * 100 + day = cast(date_format(current_date - interval '1' day, '%Y%m%d') as bigint)
              and (action in ('play', 'startWatch', 'subtitleChange', 'audioChange'))
	) pl_p
left join pl_swt_agg_by_day as pl_swt_dev_model
	 on  pl_swt_dev_model.user_uid = pl_p.user_uid
	 and pl_swt_dev_model.element_uid = pl_p.element_uid
	 and pl_swt_dev_model.device_type = pl_p.device_type
	 and pl_swt_dev_model.device_model = pl_p.device_model
	 and pl_swt_dev_model.watch_date = pl_p.watch_date

left join pl_swt_agg_by_day as pl_swt_dev_type
	 on  pl_swt_dev_type.user_uid = pl_p.user_uid
	 and pl_swt_dev_type.element_uid = pl_p.element_uid
	 and pl_swt_dev_type.device_type = pl_p.device_type
	 and pl_swt_dev_type.watch_date = pl_p.watch_date
	 and pl_swt_dev_model.view_duration is null

left join (
		select coalesce(current, user_uid) as user_uid, element_uid, consumption_mode as purchase_consumption_mode
		from transactions tr
			left join lookup.public.merged on merged.previous = tr.user_uid
		where creation_ts < current_date - interval '4' day and creation_ts >= current_date - interval '3' day 
	) ctx_trnsct
		on  ctx_trnsct.user_uid = pl_p.user_uid
		and cast(ctx_trnsct.element_uid as varchar) = coalesce(pl_p.element_uid)

left join (
		select coalesce(current, user_uid) as user_uid, subscription_uid, user_subscription_uid, actual_start_ts, actual_end_ts
		from subscriptions sub
		left join merged on merged.previous = sub.user_uid
		where actual_end_ts >= date_trunc('day', current_timestamp - interval '1' day) and actual_start_ts < date_trunc('day', current_timestamp)
	) ctx_subs
		on ctx_subs.user_uid = pl_p.user_uid
		and ctx_subs.actual_start_ts < from_unixtime(pl_p.start_timestamp)
		and ctx_subs.actual_end_ts > from_unixtime(pl_p.start_timestamp)
```

#### Есть ли в запросе логические ошибки, если есть, то какие?

При фильтрации данных в таблице transactions, используется условие

```sql
where creation_ts < current_date - interval '4' day and creation_ts >= current_date - interval '3' day
```

Данное условие будет всегда возвращать пустой результат

#### Есть ли возможность оптимизировать запрос без погружения в типы данных, наличие индексов и т.д.?

- Выравнить временные периоды. В разных подзапросах происходит работа с разными временными диапазонами. Использование одних диапазонов избавит от избыточных сравнений при join операциях.
- - pl_swt_agg_by_day данные за 7 дней
- - pl_p данные за 1 день
- Избавиться от лишнего джойна ctx_subs (не увидел использование данных из этого подзапроса)
- Возможно два джойна с pl_swt_dev_model и pl_swt_dev_type, можно заменить на один. Не совсем разобрался с тем, почему рассматриваем отдельно данные по типу устройства и по модели, если потом все равно берем данные либо от туда, либо от туда 

#### Какие данные вы получите по результатам выполнения запроса, опишите двумя словами результирующий датасет?  

Датасет показывает продолжительность просмотра некоторого контента за вчерашний день с момента таких действий как 'play', 'startWatch', 'subtitleChange', 'audioChange'. Помимо информации о продолжительности просмотра, содержится информация о пользователе, девайсе, информации о покупке