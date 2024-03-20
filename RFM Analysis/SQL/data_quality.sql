/*
* Data integrity check: checking for duplicates, out-of-range values, logical errors, etc.
* 
* Data missing analysis: determining the number and causes of data missing.
* 
* Anomalous value analysis: identifying values that are outside of expected values.
* 
* ---------------------------------------------------------------------------------------
* 
* Проверка целостности данных: проверка на наличие дубликатов, значений вне допустимого диапазона,
*  логических ошибок и т. д.
* 
* Анализ пропусков данных: определение количества и причин пропусков данных.
* 
* Анализ аномальных значений: выявление значений, которые выходят за рамки ожидаемых значений.
* 
*/


-- Data Integrity de.production

-- orders table
select *
from production.orders;

-- total records
select count(*)
from production.orders;
-- there are a total of 10000 records.

-- Missing values and nulls

-- checking for missing values in order_ts column
select count(order_ts)
from production.orders;

select *
from production.orders
where order_ts is null;
-- there are no missing values in the order_ts column.

-- checking for missing values in order_id column
select count(order_id)
from production.orders;

select *
from production.orders
where order_id is null;
-- there are no missing values in the order_id column.

-- checking for missing values in payment column
select count(payment)
from production.orders;

select *
from production.orders
where payment is null;
-- there are no missing values in the payment column.

-- checking for missing values in user_id column
select count(user_id)
from production.orders;

select *
from production.orders
where user_id is null;
-- there are no missing values in the user_id column.

-- checking for missing values in status column
select count(status)
from production.orders;

select *
from production.orders
where status  is null;
-- there are no missing values in the status column.


-- Duplicated values

--orders table
-- order_id column
select count(*) duplicate_count
from production.orders 
group by order_id
having count(*) > 1;


select distinct on (order_id) order_id,
                   order_ts 
from production.orders
order by order_id ,
         order_ts; 
-- there are no order_id duplicates


-- Value range

-- checking order_id values
select min(order_id), max(order_id)
from production.orders;
-- the values seems to be legit. no negative values.

-- order_ts depth
select min(order_ts), max(order_ts)
from production.orders;
-- the min date is 2022-02-12 and the max date is 2022-03-14
-- there are only a ~month worth of data.

-- checking user_id values
select min(user_id), max(user_id)
from production.orders;
-- there are users with id = 0.

-- checking the amount of users
select count(distinct user_id)
from production.orders;
-- there is a total of 1000 users.

-- checking how many users have id = 0
select count(user_id)
from production.orders
where user_id = 0;
-- there are 13 orders with user_id = 0

-- checking the average orders of a user 
select
	avg(t.cnt)
from(select user_id, 
		count(order_id) cnt
	from production.orders
	group by user_id) t;
-- the average orders amount is 10 so a user with id = 0 might be valid.

-- to be sure, checking the user with id = 0 form users table.
select *
from production.users
where id = 0;
-- there is only 1 user with id = 0.
-- noticed the values of login and name columns in the users table are mixed:
-- the name is in the login column and the login is in the name column.
-- since we dont need this table for out tank we wont be touching it.

-- checking the lowest and highest payment amount
select min(payment), max(payment) 
from production.orders;
-- the min payment amount is 60, the max payments amount is 6360.

-- checking the lowest and the highest price for a single product
select min(price), max(price)
from production.products;
-- the lowest price  for a single product is 60 so the min price in the orders table is valid.

-- checking how many orders are with the status "closed"
-- first we would get the correct number value for status "Closed" from orderstatuses table
select *
from production.orderstatuses
where "key" = 'Closed';
-- the "Closed" status represented as 4.

-- now counting the orders with status = 4
select count(status)
from production.orders
where status = 4;
-- there are 4991 ~half the orders are "Closed"

-- Now counting the number of users that have "Closed" orders
select count(distinct user_id)
from production.orders
where status = 4;
-- there are 988 users with "Closed" orders.

--checking if there are statuses out of 1-5 range
select *
from production.orders
where status not in (1,2,3,4,5);
-- there are no out of range errors in status column.

-- Checking if all the user ids from orders table are present in the users table
select count(user_id)
from production.orders
where user_id not in (select id
			  from production.users)
-- all the users from orders table are present is users table.

/*
 * Summary:
 
Data Integrity Verification:

Missing Values: The orders table exhibits no missing values in the order_ts, order_id, 
payment, user_id, or status columns.

Duplicate Values: No duplicate order_id values were detected in the orders table.


Data Range and Value Distribution Analysis:

Order IDs: The order_id values fall within a valid range, with no negative values.

Order Timestamps: The order_ts values span approximately one month, 
from February 12, 2022, to March 14, 2022.

User IDs: The user_id values include a user with ID 0. Further investigation revealed
that this user is valid, with an average number of orders comparable to other users 
and a total of 13 orders with user ID 0.

Payment Amounts: The payment amounts range from 60 to 6360, aligning with the minimum
and maximum product prices.

Order Statuses: The status values adhere to the valid range of 1 to 5, 
with no out-of-range errors.


Additional Observations and Recommendations:

Order Status Distribution: Approximately half of the orders have the status "Closed".

Users with "Closed" Orders: 988 users have placed orders with the status "Closed".



User Table Inconsistencies: The users table appears to have inconsistencies in the
login and name columns. Since this table is not directly relevant to the current task, 
it will not be addressed at this time. Implementing a data validation process could prevent 
the introduction of inconsistent data in the future.

Overall, the orders table exhibits good data integrity and falls within expected value ranges.
The observed patterns and distributions align with reasonable business scenarios. 

--------------------------------------------------------------------------------

Проверка целостности данных:

Отсутствующие значения: В таблице заказов нету отсутствующих значений в столбцах order_ts, 
order_id, payment, user_id и status.

Дубликаты: В таблице заказов не обнаружено дубликатов order_id.


Анализ диапазона данных и распределения значений:

Идентификаторы заказов: Значения order_id находятся в допустимом диапазоне без
отрицательных значений.

Временные метки заказов: Значения order_ts охватывают примерно один месяц, 
с 12 февраля 2022 года по 14 марта 2022 года.

Идентификаторы пользователей: Значения user_id включают пользователя с идентификатором 0.
Дальнейшее расследование показало, что этот пользователь является действительным,
с средним количеством заказов, сравнимым с другими пользователями, 
и в общей сложности 13 заказов с идентификатором пользователя 0.

Суммы платежей: Суммы платежей варьируются от 60 до 6360, что соответствует
минимальным и максимальным ценам на продукты.

Статусы заказов: Значения статуса соответствуют допустимому диапазону от 1 до 5 без ошибок
выхода за пределы диапазона.


Дополнительные наблюдения и рекомендации:

Распределение статусов заказов: Примерно половина заказов имеют статус "Закрыто".

Пользователи с заказами со статусом "Закрыто": У 988 пользователей есть заказы 
со статусом "Закрыто".

Несоответствия в таблице пользователей: В таблице пользователей, похоже, 
есть несоответствия в столбцах login и name. Поскольку эта таблица напрямую не связана 
с текущей задачей, она не будет рассматриваться в настоящее время. Внедрение процесса валидации 
данных может предотвратить ввод недопустимых данных в будущем.

В целом таблица заказов демонстрирует хорошую целостность данных и попадает в ожидаемые
диапазоны значений. Наблюдаемые закономерности и распределения соответствуют разумным 
бизнес-сценариям.
 */





