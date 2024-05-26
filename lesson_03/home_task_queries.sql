/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
select count(film_id), category_id
from film_category
group by category_id
order by 1 desc;



/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
select a.actor_id, a.first_name, a.last_name, count(*) from rental r
join inventory i on r.inventory_id = i.inventory_id
join film_actor fa on i.film_id = fa.film_id
join actor a on fa.actor_id = a.actor_id
group by 1, 2, 3
order by 4 desc
limit 10;



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
select c.name, sum(p.amount) from category c
join film_category fc on c.category_id = c.category_id
join inventory i on fc.film_id = i.film_id
join rental r on i.inventory_id = r.inventory_id
join payment p on r.rental_id  = p.rental_id
group by 1
order by 2 desc
limit 1;



/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
select f.film_id, f.title from film f
left join inventory i on f.film_id = i.film_id
where i.film_id is null;



/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
select a.first_name, a.last_name, count(*) from category c
join film_category fc on fc.category_id = c.category_id
join film_actor fa on fa.film_id = fc.film_id
join actor a on a.actor_id = fa.actor_id
where c.name = 'Children'
group by 1, 2
order by 3 desc
limit 3;
