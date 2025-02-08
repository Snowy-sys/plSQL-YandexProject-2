# plSQL-YandexProject-2
Самостоятельный проект по итогу окончания курса "Продвинутый SQL для работы с данными"

## Ревью
Результат ревью и отзыв по проекту размещен в [Comments.txt]

## Описание проекта. Сеть ресторанов Gastro Hub.
Вы работаете в Gastro Hub — динамично развивающейся корпорации, которая управляет заведениями общественного питания в центре Москвы. Gastro Hub известна своими ресторанами, барами, пиццериями и кофейнями — у каждого заведения уникальная атмосфера и концепция. Компания постоянно исследует новые кулинарные тренды и заботится о том, чтобы гости каждого заведения наслаждались высоким качеством сервиса и незабываемым вкусом блюд.

Pasta Palace славится традиционной итальянской кухней в сердце города. Spirits & Spices, расположенный на знаменитой улице с барами, привлекает молодежь вкусными коктейлями и живой музыкой. Dough & Cheese — любимое место многих за непревзойдённые пиццы, а в Hops Heaven можно попробовать отборный кофе, любуясь на городской пейзаж.

Грамотный менеджмент требует слаженной работы и чёткого управления информацией. Ваша задача — создать базу данных, которая поможет Gastro Hub ещё лучше понимать свой бизнес и оптимизировать работу компании. 
Для этого необходимо работать с большим объёмом данных: информацией о заведениях, меню, менеджерах, среднем чеке и расположении заведений. Вы получите сырые, необработанные данные, по которым построите дополнительные таблицы, представления и материализованные представления, а также напишите несколько аналитических запросов. 

Также вы получите дамп — резервную копию базы данных в двух форматах. Чтобы загрузить архивный файл с расширением ```.sql```, используйте функцию pgAdmin ```Restore```. Чтобы дамп успешно развернулся, на сервере PostgreSQL должен быть установлен PostGIS. Если у вас возникнут проблемы с разворачиванием дампа, воспользуйтесь версией в текстовом формате — файлом с расширением ```.zip```. Это архив — распакуйте его, а затем используйте файл.

### Цель проекта
Построить дополнительные таблицы с продвинутыми типами данных и выполнить семь заданий: поработать с геоданными, создадь представления и написать несколько аналитических запросов, используя оконные функции и подзапросы.

#### Предусловия
Перед началом выполнения заданий необходимо развернуть базу данных из дампа [] и выполнить проект в два этапа. Этап №1: создание дополнительных таблиц. Этап №2: создание представлений и написание аналитических запросов. Оба этапа подробно описаны в [СР_2]

#### Задание №1
Чтобы выдать премию менеджерам, нужно понять, у каких заведений самый высокий средний чек. Создайте представление, которое покажет топ-3 заведений внутри каждого типа заведения по среднему чеку за все даты. Столбец со средним чеком округлите до второго знака после запятой.
```sql
CREATE VIEW v_top_check as
with top_1 as (
with top as (
SELECT 
    cr.name as name,
    cr.restaurant_type as type,
    round(avg(avg_check), 2) as avg_check
from cafe.sales cs
join cafe.restaurants cr on cs.restaurant_uuid = cr.restaurant_uuid 
group by name, type)
SELECT
	top.avg_check, 
	top.name, 
	row_number()over(partition by top.type order by top.avg_check desc) as num,
	top.type
from top
order by top.type, top.avg_check desc)
select *
from top_1
where top_1.num = 1 or top_1.num = 2 or top_1.num = 3;

--Проверка задания №1
select * from v_top_check;
```

#### Задание №2
Создайте материализованное представление, которое покажет, как изменяется средний чек для каждого заведения от года к году за все года за исключением 2023 года. Все столбцы со средним чеком округлите до второго знака после запятой.
```sql
CREATE MATERIALIZED VIEW check_avg_change as
with top as (
SELECT
	extract (year from date) as year, 
	avg(avg_check) as av_ch, 
	restaurant_uuid as rest_uuid
from cafe.sales
where extract (year from date) between '2017' and '2022'
group by rest_uuid, year	
)
SELECT
	top.year, 
	top.av_ch, 
	lag(top.av_ch)over(partition by cr.name order by top.year) as avg_last,
	round(((top.av_ch / lag(top.av_ch)over(partition by cr.name order by top.year) -1) *100), 2) as proz,
	cr.name, 
	cr.restaurant_type
from top
left join cafe.restaurants cr on top.rest_uuid = cr.restaurant_uuid;

--Проверка задания №2
select * from check_avg_change order by year;
```

#### Задание №3
Найдите топ-3 заведения, где чаще всего менялся менеджер за весь период.
```sql
select 
	count(distinct сm.manager_uuid) as man, 
	сr.name as rest
from cafe.restaurant_manager_work_dates as сm
left join cafe.restaurants as сr on сm.restaurant_uuid = сr.restaurant_uuid
group by сr.name
order by man desc
limit 3;
```

#### Задание №4
Найдите пиццерию с самым большим количеством пицц в меню. Если таких пиццерий несколько, выведите все.
```sql
with top_1 as (
SELECT 
	top.name as name_cafe, 
	count(top.pizza) as count_pizza
from (
SELECT
	name as name, 
	JSONB_EACH_TEXT(menu::jsonb -> 'Пицца')::text as pizza
from cafe.restaurants
) as top
group by top.name
order by count_pizza desc)
SELECT
	*, 
	dense_rank() over (order by top_1.count_pizza desc) as rank_of_pizza
from top_1
limit 3;
```

#### Задание №5
Найдите самую дорогую пиццу для каждой пиццерии.
```sql
with top_1 as (
with top as (
SELECT 
	name as rest_name, 
	restaurant_type as type, 
	jsonb_object_keys(menu -> 'Пицца') as piz_name, 
	((menu -> 'Пицца') ->> (jsonb_object_keys(menu -> 'Пицца')))::numeric(6,0) as piz_price
from cafe.restaurants
where restaurant_type = 'pizzeria')
SELECT 
	*, 
	row_number()over(partition by top.rest_name order by piz_price desc) as menu_rank
from top
)
SELECT 
	rest_name, 
	type, 
	piz_name, 
	piz_price
from top_1
where top_1.menu_rank = 1;
```

#### Задание №6
Найдите два самых близких друг к другу заведения одного типа.
```sql
with top_1 as(
with top as (
SELECT
	cr1.name as name_1, 
	cr1.rest_location::geography as location_1, 
	cr1.restaurant_type as rest_type,
	cr2.name as name_2, 
	cr2.rest_location::geography as location_2  
from cafe.restaurants as cr1
join cafe.restaurants as cr2 on cr1.restaurant_type = cr2.restaurant_type
)
SELECT
	top.name_1 as n1, 
	top.rest_type as rt, 
	ST_Distance(location_1, location_2) as distance,
	top.name_2 as n2
from top
)
SELECT 
	top_1.n1, 
	top_1.rt, 
	min(top_1.distance) as min_dist, 
	top_1.n2
from top_1
group by top_1.n1, top_1.rt, top_1.n2
having min(top_1.distance) > 0
order by min(top_1.distance)
limit 1;
```

#### Задание №7
Найдите район с самым большим количеством заведений и район с самым маленьким количеством заведений. Первой строчкой выведите район с самым большим количеством заведений, второй — с самым маленьким. 
```sql
with top as (
SELECT 
	count(cr.name) as count_rest, 
	dis.district_name as dis_name
from cafe.districts as dis
join cafe.restaurants as cr on ST_Within(cr.rest_location, dis.district_geom)
group by dis_name)
(SELECT
 	top.dis_name, 
 	min(top.count_rest) as max_count_rest
from top
group by top.dis_name
order by min(top.count_rest) asc
limit 1)
UNION
(SELECT
	top.dis_name, 
	min(top.count_rest) as min_count_rest
from top
group by top.dis_name
order by min(top.count_rest) desc
limit 1)
order by max_count_rest desc;
```
