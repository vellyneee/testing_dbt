
{{ config(materialized='table') }}


with source_customers as (
    select * from {{ source('test_dbt', 'customers') }}
),

source_orders as (
    select * from {{ source('test_dbt', 'orders') }}
),

source_products as (
    select * from {{ source('test_dbt', 'products') }}
),

source_sales as (
    select * from {{ source('test_dbt', 'sales') }}
),

result as (
select *,
case
    when orders.count = 0
    then 'Newbie'
    when orders.count > 0 and orders.count <= 10
    then 'Silver'
    when orders.count > 10 and orders.count <= 20
    then 'Gold'
    when orders.count > 20
    then 'Platinum'
end
as membership

from
(
    select count(orders.order_id) as count
    from customers
    join orders
    on customers.customer_id = orders.customer_id
    join sales
    on orders.order_id = sales.order_id
    join products
    on sales.product_id = products.product_id
    group by orders.customer_id
) orders,

(
    select customer_name, gender, age, home_address, zip_code, city, state, country,
    payment, order_date, delivery_date,
    price_per_unit, total_price,
    product_type, product_name, size, colour, price, description
    from customers
    join orders
    on customers.customer_id = orders.customer_id
    join sales
    on orders.order_id = sales.order_id
    join products
    on sales.product_id = products.product_id
) as data

order by orders.count desc
)

select * from result