# Витрина RFM
---


## 1.1. Выясните требования к целевой витрине.

### Создать витрину данных для RFM-классификации пользователей приложения:

    Название витрины: dm_rfm_segments;
    Расположение витрины: de.analysis;
    Расположение данных: de.production;
    Структура витрины:
        user_id
        recency (число от 1 до 5) - Cколько времени прошло с момента последнего заказа.
        frequency (число от 1 до 5) - Количество заказов.
        monetary_value (число от 1 до 5) - Сумма затрат клиента.
    Глубина витрины: 2022/01/01 - 2022/12/31;
    Обновления не нужны;
    Успешно выполненый заказ это заказ со статусом "Closed";
    Разделить пользователей на 5 равных RMF сегментов, если количество заказов одинаковое,
    без разницы кого ставить выше, кого ниже.

## 1.2. Изучите структуру исходных данных.

### Требования к витрине RFM-классификации пользователей приложения

#### Исходные данные

База данных: `de.production`

Таблицы:
`orders` - заказы
`products` - товары
`orderitems` - заказанные товары
`orderstatuslog` - логи статусов
`orderstatuses` - статусы
`users` - пользователи

Поля:
- **`orders`**:
	- `order_id` - int4 NOT NULL
	- `order_ts` - timestamp NOT NULL
	- `user_id` - int4 NOT NULL
	- `bonus_payment` - numeric(19, 5) NOT NULL DEFAULT 0
	- `payment` - numeric(19, 5) NOT NULL DEFAULT 0
	- `"cost"` - numeric(19, 5) NOT NULL DEFAULT 0
	- `bonus_grant` - numeric(19, 5) NOT NULL DEFAULT 0
	- `status` - int4 NOT NULL

- **`products`**:
	- `id` - int4 NOT NULL
	- `"name"` - varchar(2048) NOT NULL
	- `price` - numeric(19, 5) NOT NULL DEFAULT 0

- **`orderitems`**:
	- `id` - int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE)
	- `product_id` - int4 NOT NULL
	- `order_id` - int4 NOT NULL
	- `"name"` - varchar(2048) NOT NULL
	- `price` - numeric(19, 5) NOT NULL DEFAULT 0
	- `discount` - numeric(19, 5) NOT NULL DEFAULT 0
	- `quantity` - int4 NOT NULL

- **`orderstatuslog`**:
	- `id` - int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE)
	- `order_id` - int4 NOT NULL
	- `status_id` - int4 NOT NULL
	- `dttm` - timestamp NOT NULL

- **`orderstatuses`**:
	- `id` - int4 NOT NULL
	- `"key"` - varchar(255) NOT NULL

- **`users`**:
	- `id` - int4 NOT NULL
	- `"name"` - varchar(2048) NULL
	- `login` - varchar(2048) NOT NULL


#### Выходные данные

Визуализация: таблица с данными о RFM-сегментах пользователей
Расчёт RFM-сегментов
- `user_id` идентификатор пользователя
- `recency`: количество дней, прошедших с момента последнего заказа
- `frequency`: количество заказов
- `monetary_value`: сумма затрат

Используемые поля:
- **`orders`**:
	- `order_id` - идентификатор заказа
	- `order_ts` - дата заказа
	- `user_id` - идентификатор клиента
	- `payment` - сумма оплаты
	- `status` - статус заказа

Глубина витрины: 2022/01/01 - 2022/12/31

Обновления не нужны

Успешно выполненный заказ это заказ со статусом "Closed"

Разделить пользователей на 5 равных RMF сегментов, если количество заказов одинаковое, без разницы кого ставить выше, кого ниже.


## 1.3. Проанализируйте качество данных

- Проверка целостности данных: проверка на наличие дубликатов, значений вне допустимого диапазона, логических ошибок.
- Анализ пропусков данных: определение количества и причин пропусков данных.
- Анализ аномальных значений: выявление значений, которые выходят за рамки ожидаемых значений.

### Проверка целостности данных:

Отсутствующие значения: В таблице заказов нету отсутствующих значений в столбцах ```order_ts```, 
```order_id```, ```payment```, ```user_id``` и ```status```.

Дубликаты: В таблице заказов не обнаружено дубликатов ```order_id```.


### Анализ диапазона данных и распределения значений:

- Идентификаторы заказов: Значения ```order_id``` находятся в допустимом диапазоне без
отрицательных значений.
- Временные метки заказов: Значения ```order_ts``` охватывают примерно один месяц, 
с 12 февраля 2022 года по 14 марта 2022 года.
- Идентификаторы пользователей: Значения ```user_id``` включают пользователя с идентификатором 0.
Дальнейшее расследование показало, что этот пользователь является действительным,
с средним количеством заказов, сравнимым с другими пользователями, 
и в общей сложности 13 заказов с идентификатором пользователя = 0.
- Суммы платежей: Суммы платежей варьируются от 60 до 6360, что соответствует минимальным и максимальным ценам на продукты.
- Статусы заказов: Значения статуса соответствуют допустимому диапазону от 1 до 5 без ошибок выхода за пределы диапазона.


### Дополнительные наблюдения и рекомендации:
- Распределение статусов заказов: Примерно половина заказов имеют статус ```"Закрыто"```.
- Пользователи с заказами со статусом "Закрыто": У 988 пользователей есть заказы 
со статусом ```"Закрыто"```.
- Несоответствия в таблице пользователей: В таблице пользователей, похоже, 
есть несоответствия в столбцах ```login``` и ```name```. Поскольку эта таблица напрямую не связана 
с текущей задачей, она не будет рассматриваться в настоящее время. Внедрение процесса валидации 
данных может предотвратить ввод недопустимых данных в будущем.

В целом таблица заказов демонстрирует хорошую целостность данных и попадает в ожидаемые
диапазоны значений. Наблюдаемые закономерности и распределения соответствуют разумным 
бизнес-сценариям.


## Укажите, какие инструменты обеспечивают качество данных в источнике.
Ответ запишите в формате таблицы со следующими столбцами:
- `Наименование таблицы` - наименование таблицы, объект которой рассматриваете.
- `Объект` - Здесь укажите название объекта в таблице, на который применён инструмент. Например, здесь стоит перечислить поля таблицы, индексы и т.д.
- `Инструмент` - тип инструмента: первичный ключ, ограничение или что-то ещё.
- `Для чего используется` - здесь в свободной форме опишите, что инструмент делает.

| **Таблицы**               | **Объект**                                                                                                             | **Инструмент**   | **Для чего используется**                                                   |
|---------------------------|------------------------------------------------------------------------------------------------------------------------|------------------|-----------------------------------------------------------------------------|
| production.orderitems     | id int4 NOT NULL PRIMARY KEY                                                                                           | _Первичный ключ_ | Обеспечивает уникальность записей о товарах в заказе                        |
| production.orderitems     | order_id int4 NOT NULL                                                                                                 | _Индекс_         | Ускоряет поиск записей о товарах в заказе по номеру заказа                  |
| production.orderitems     | product_id int4 NOT NULL                                                                                               | _Индекс_         | Ускоряет поиск записей о товарах в заказе по коду товара                    |
| production.orderitems     | discount numeric(19, 5) NOT NULL DEFAULT 0                                                                             | _Ограничение_    | Ограничивает значение скидки от 0 до цены товара                            |
| production.orderitems     | price numeric(19, 5) NOT NULL DEFAULT 0                                                                                | _Ограничение_    | Ограничивает значение цены товара от 0                                      |
| production.orderitems     | quantity int4 NOT NULL                                                                                                 | _Ограничение_    | Ограничивает количество товара в заказе от 1                                |
| production.orders         | order_id int4 NOT NULL PRIMARY KEY                                                                                     | _Первичный ключ_ | Обеспечивает уникальность записей о заказах                                 |
| production.orders         | order_ts timestamp NOT NULL                                                                                            | _Ограничение_    | Ограничивает значение даты заказа на не более текущей даты                  |
| production.orders         | user_id int4 NOT NULL                                                                                                  | _Индекс_         | Ускоряет поиск записей о заказах по коду пользователя                       |
| production.orders         | bonus_payment numeric(19, 5) NOT NULL DEFAULT 0                                                                        | _Ограничение_    | Ограничивает значение бонусного платежа от 0                                |
| production.orders         | payment numeric(19, 5) NOT NULL DEFAULT 0                                                                              | _Ограничение_    | Ограничивает значение платежа от 0                                          |
| production.orders         | "cost" numeric(19, 5) NOT NULL DEFAULT 0                                                                               | _Ограничение_    | Ограничивает значение стоимости заказа на сумму платежа + бонусного платежа |
| production.orders         | bonus_grant numeric(19, 5) NOT NULL DEFAULT 0                                                                          | _Ограничение_    | Ограничивает значение бонусного начисления от 0                             |
| production.orders         | status int4 NOT NULL                                                                                                   | _Индекс_         | Ускоряет поиск записей о заказах по статусу                                 |
| production.orderstatuses  | id int4 NOT NULL PRIMARY KEY                                                                                           | _Первичный ключ_ | Обеспечивает уникальность записей о статусах заказов                        |
| production.orderstatuslog | id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) | _Первичный ключ_ | Обеспечивает уникальность записей о журналах статусов заказов               |
| production.orderstatuslog | order_id int4 NOT NULL                                                                                                 | _Индекс_         | Ускоряет поиск записей о журналах статусов заказов по номеру заказа         |
| production.orderstatuslog | status_id int4 NOT NULL                                                                                                | _Индекс_         | Ускоряет поиск записей о журналах статусов заказов по коду статуса          |
| production.products       | id int4 NOT NULL PRIMARY KEY                                                                                           | _Первичный ключ_ | Обеспечивает уникальность записей о товарах                                 |
| production.products       | "name" varchar(2048) NOT NULL                                                                                          | _Индекс_         | Ускоряет поиск записей о товарах по названию                                |
| production.products       | price numeric(19, 5) NOT NULL DEFAULT 0                                                                                | _Ограничение_    | Ограничивает значение цены товара от 0                                      |
| production.users          | id int4 NOT NULL PRIMARY KEY                                                                                           | _Первичный ключ_ | Обеспечивает уникальность записей о пользователях                           |
| production.users          | "name" varchar(2048) NULL                                                                                              | _Индекс_         | Ускоряет поиск записей о пользователях по имени                             |
| production.users          | login varchar(2048) NOT NULL                                                                                           | _Индекс_         | Ускоряет поиск записей о пользователях по логину                            |



## 1.4. Подготовьте витрину данных

### 1.4.1. Сделайте VIEW для таблиц из базы production.**

{См. задание на платформе}
```SQL
-- Создание представления Users
--drop view if exists analysis.users;
create view analysis.users as
select *
from production.users;

-- Создание представления OrderItems
--drop view if exists analysis.orderitems;
create view analysis.orderitems as
select *
from production.orderitems;

-- Создание представления OrderStatuses
--drop view if exists analysis.orderstatuses;
create view analysis.orderstatuses as
select *
from production.orderstatuses;

-- Создание представления Products
--drop view if exists analysis.products;
create view analysis.products as
select *
from production.products;

-- Создание представления Orders
--drop view if exists analysis.orders;
create view analysis.orders as
select *
from production.orders;
```

### 1.4.2. Напишите DDL-запрос для создания витрины.**

```SQL
--Data mart ddl

create table if not exists dm_rfm_segments (
user_id int4 not null,
recency int4 not null,
frequency int4 not null,
monetary_value int4 not null,

constraint dm_rfm_segments_user_id_pk primary key(user_id),
constraint dm_rfm_segments_recency_check check(recency >= 1 and recency <= 5),
constraint dm_rfm_segments_frequency_check check(frequency >= 1 and frequency <= 5),
constraint dm_rfm_segments_monetary_value_check check(monetary_value >= 1 and monetary_value <= 5)
);


-- checking unique index
select indexdef 
from pg_catalog.pg_indexes
where tablename = 'dm_rfm_segments';
```

### 1.4.3. Напишите SQL запрос для заполнения витрины

```SQL
-- tmp_rfm_recency table fill

-- truncate table analysis.tmp_rfm_recency;
insert into analysis.tmp_rfm_recency
with r as (select user_id,
ntile(5) over (order by day_recency desc) recency
--case 
--	when percent_rank() over (order by day_recency) <= 0.2 then 5 
--	when percent_rank() over (order by day_recency) <= 0.4 then 4
--	when percent_rank() over (order by day_recency) <= 0.6 then 3
--	when percent_rank() over (order by day_recency) <= 0.8 then 2
--	else 1
--end recency
from (select user_id,
	  		 date_part('day', current_timestamp - max(order_ts)) day_recency
      from analysis.orders
	  where status = 4
	  group by user_id) t
),

u as (
select distinct on (id) id
from analysis.users
)

select u.id,
	   coalesce(r.recency, 1)
from u
left join r on u.id = r.user_id;


-- tmp_rfm_frequency table fill

--truncate table analysis.tmp_rfm_frequency;
insert into analysis.tmp_rfm_frequency
with f as (select user_id,
ntile(5) over (order by total_orders) frequency
--case 
--	when percent_rank() over (order by total_orders) <= 0.2 then 1
--	when percent_rank() over (order by total_orders) <= 0.4 then 2
--	when percent_rank() over (order by total_orders) <= 0.6 then 3
--	when percent_rank() over (order by total_orders) <= 0.8 then 4
--	else 5
--end frequency
from (select user_id,
	  		 count(order_id) total_orders
	  from analysis.orders
	  where status = 4
      group by user_id) t
),

u as (select distinct on (id) id
from analysis.users
)

select u.id,
	   coalesce(f.frequency, 1)
from u 
left join f on u.id = f.user_id;



-- tmp_rfm_monetary_value table fill

--truncate table analysis.tmp_rfm_monetary_value;
insert into analysis.tmp_rfm_monetary_value 
with m as (
select user_id,
ntile(5) over (order by total_payment_amount) monetary_value
--case 
--	when percent_rank() over (order by total_payment_amount) <= 0.2 then 1
--	when percent_rank() over (order by total_payment_amount) <= 0.4 then 2
--	when percent_rank() over (order by total_payment_amount) <= 0.6 then 3
--	when percent_rank() over (order by total_payment_amount) <= 0.8 then 4
--	else 5
--end monetary_value
from (select user_id,
	   sum(payment) total_payment_amount
from analysis.orders
where status = 4
group by user_id) t
),

u as (
select distinct on (id) id 
from analysis.users
)

select u.id,
	   coalesce(monetary_value, 1)
from u 
left join m on u.id = m.user_id;





-- dm_rfm_segments table fill

--truncate table analysis.dm_rfm_segments;
insert into analysis.dm_rfm_segments
select trr.user_id, 
	   trr.recency,
	   trf.frequency,
	   trmv.monetary_value
from analysis.tmp_rfm_recency trr 
left join analysis.tmp_rfm_frequency trf 
	on trr.user_id = trf.user_id
left join analysis.tmp_rfm_monetary_value trmv
	on trmv.user_id = trr.user_id;


-- 10 first rows
select *
from analysis.dm_rfm_segments drs
order by user_id
limit 10;

ser_id|recency|frequency|monetary_value|
------+-------+---------+--------------+
     0|      1|        3|             4|
     1|      4|        3|             3|
     2|      2|        3|             5|
     3|      2|        3|             3|
     4|      4|        3|             3|
     5|      5|        5|             5|
     6|      1|        3|             5|
     7|      4|        2|             2|
     8|      1|        1|             3|
     9|      2|        2|             2|

```



