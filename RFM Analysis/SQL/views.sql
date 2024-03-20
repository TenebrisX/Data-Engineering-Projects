/*
Five views (one for each table):
This views would contain all of the columns from their tables.
Users,
OrderItems,
OrderStatuses,
Products,
Orders.

Пять представлений (по одному на каждую таблицу):
Users,
OrderItems,
OrderStatuses,
Products,
Orders.
*/

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

