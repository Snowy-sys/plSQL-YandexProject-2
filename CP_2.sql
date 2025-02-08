/*Этап 1. Создание дополнительных таблиц*/
--Шаг 1
--Создание типа данных enum
create type cafe.restaurant_type as enum 
('coffee_shop', 'restaurant', 'bar', 'pizzeria'); 


--Шаг 2
--Создание таблицы cafe.restaurants
create table cafe.restaurants (
    restaurant_uuid uuid PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
	name VARCHAR(100) not null,
	rest_location GEOMETRY (point, 4326),
	restaurant_type cafe.restaurant_type,
	menu jsonb); 

--Заполнение данных таблицы cafe.restaurants
insert into cafe.restaurants (name, rest_location, restaurant_type, menu)
select 	
	distinct (rds.cafe_name), 
	CONCAT('POINT (', rds.longitude, ' ', rds.latitude, ')'), 
	rds.type::cafe.restaurant_type, 
	rdm.menu
from raw_data.sales rds
left join raw_data.menu rdm on rds.cafe_name = rdm.cafe_name; 

--Шаг 3
--Создание таблицы cafe.managers
create table cafe.managers (
	manager_uuid uuid PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
	manager_name VARCHAR(100) not null,
	phone VARCHAR(30)); 

--Заполнение таблицы cafe.managers
insert into cafe.managers (manager_name, phone)
select 
	distinct (manager), 
	manager_phone
from raw_data.sales;


--Шаг 4
--Создание таблицы cafe.restaurant_manager_work_dates
create table cafe.restaurant_manager_work_dates (
	start_day date not null,
	end_day date,
	PRIMARY KEY (restaurant_uuid, manager_uuid),
	restaurant_uuid uuid constraint fk_restaurant_manager_work_dates_restaurant_restaurant_uuid references cafe.restaurants,
	manager_uuid uuid constraint fk_restaurant_manager_work_dates_manager_manager_uuid references cafe.managers
	); 

--Заполнение данных таблицы cafe.restaurant_manager_work_dates
insert into cafe.restaurant_manager_work_dates (restaurant_uuid, manager_uuid, start_day, end_day)
select 
	cr.restaurant_uuid as restaurant_uuid, 
	cm.manager_uuid as manager_uuid, 
	min(rds.report_date) as start_day, 
	max(rds.report_date) as end_day
from raw_data.sales rds
join cafe.restaurants cr on rds.cafe_name = cr.name
join cafe.managers cm on rds.manager = cm.manager_name
group by restaurant_uuid, manager_uuid; 

--Шаг 5
--Создание таблицы cafe.sales
create table cafe.sales (
	date date not null,
	restaurant_uuid uuid not null,
	avg_check numeric(6,2) not null,
	PRIMARY KEY (date, restaurant_uuid),
	constraint fk_restaurant foreign key (restaurant_uuid) references cafe.restaurants
); 

--Заполнение данных таблицы cafe.sales
insert into cafe.sales (date, restaurant_uuid, avg_check)
select
    s.report_date,
    r.restaurant_uuid,
	s.avg_check
from raw_data.sales as s
left join cafe.restaurants r on s.cafe_name = r.name; 


/*Задание 1*/
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


/*Задание 2*/
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


/*Задание 3*/
select 
	count(distinct сm.manager_uuid) as man, 
	сr.name as rest
from cafe.restaurant_manager_work_dates as сm
left join cafe.restaurants as сr on сm.restaurant_uuid = сr.restaurant_uuid
group by сr.name
order by man desc
limit 3;

/*Задание 4*/
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


/*Задание 5*/
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


/*Задание 6*/
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


/*Задание 7*/
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


