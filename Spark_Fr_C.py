#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, avg, min
from pyspark.sql.functions import sum
from pyspark.sql.functions import when
from pyspark.sql.functions import count
from pyspark.sql.window import Window
from pyspark.sql.functions import desc
from pyspark.sql.functions import expr
from pyspark.sql.functions import col, round, expr, dayofmonth, hour, minute, datediff, unix_timestamp


# In[3]:


spark = SparkSession.builder \
    .config("spark.jars", "D:\Rep\INNOWISE\Study\TASK5\postgresql-42.6.0.jar") \
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.memoryOverhead", "1g") \
    .appName("SqlToSpark") \
    .getOrCreate()


# In[4]:


spark


# In[5]:


url = "jdbc:postgresql://localhost:5432/postgres"


# In[6]:


properties = {
    "user": "postgres",
    "password": "******",
    "driver": "org.postgresql.Driver"
}


# In[7]:


film_df = spark.read.jdbc(url, "film", properties=properties)


# In[8]:


film_category_df = spark.read.jdbc(url, "film_category", properties=properties)


# In[9]:


actor_df = spark.read.jdbc(url, "actor", properties=properties)


# In[10]:


film_actor_df = spark.read.jdbc(url, "film_actor", properties=properties)


# In[11]:


category_df = spark.read.jdbc(url, "category", properties=properties)


# In[12]:


inventory_df = spark.read.jdbc(url, "inventory", properties=properties)


# In[13]:


rental_df = spark.read.jdbc(url, "rental", properties=properties)


# In[14]:


payment_df = spark.read.jdbc(url, "payment", properties=properties)


# In[15]:


city_df = spark.read.jdbc(url, "city", properties=properties)


# In[16]:


address_df = spark.read.jdbc(url, "address", properties=properties)


# In[17]:


customer_df = spark.read.jdbc(url, "customer", properties=properties)


# In[18]:


# 1. Вывести количество фильмов в каждой категории, отсортировать по убыванию
films_in_category = film_df \
    .join(film_category_df, film_df.film_id == film_category_df.film_id) \
    .join(category_df, film_category_df.category_id == category_df.category_id) \
    .groupBy(category_df.name.alias("category_name")) \
    .count() \
    .orderBy("count", ascending=False)


# In[19]:


films_in_category.show(truncate = False)


# In[20]:


# 2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию
top_actors_rentals = actor_df \
    .join(film_actor_df, actor_df.actor_id == film_actor_df.actor_id) \
    .join(film_df, film_actor_df.film_id == film_df.film_id) \
    .join(inventory_df, film_df.film_id == inventory_df.film_id) \
    .join(rental_df, inventory_df.inventory_id == rental_df.inventory_id) \
    .filter(rental_df.return_date.isNotNull() & rental_df.rental_date.isNotNull()) \
    .withColumn("rental_duration", (rental_df.return_date - rental_df.rental_date)) \
    .selectExpr("CONCAT_WS(' ', COALESCE(first_name, ''), COALESCE(last_name, '')) AS actor_name", "rental_duration") \
    .orderBy("rental_duration", ascending=False) \
    .limit(10)


# In[21]:


top_actors_rentals.show(truncate = False)


# In[22]:


# 3. Вывести категорию фильмов, на которую потратили больше всего денег
category_spending = category_df \
    .join(film_category_df, category_df.category_id == film_category_df.category_id) \
    .join(film_df, film_category_df.film_id == film_df.film_id) \
    .join(inventory_df, film_df.film_id == inventory_df.film_id) \
    .join(rental_df, inventory_df.inventory_id == rental_df.inventory_id) \
    .join(payment_df, rental_df.rental_id == payment_df.rental_id) \
    .groupBy(category_df.name.alias("category_name")) \
    .sum("amount") \
    .orderBy("sum(amount)", ascending=False) \
    .limit(1)


# In[23]:


category_spending.show(truncate = False)


# In[24]:


# 4. Вывести названия фильмов, которых нет в inventory
missing_films = film_df \
    .join(inventory_df, film_df.film_id == inventory_df.film_id, "left") \
    .filter(inventory_df.film_id.isNull()) \
    .select(film_df.film_id, film_df.title) \
    .orderBy("title")


# In[25]:


missing_films.show(50, truncate = False)


# In[26]:


# 5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории "Children"
top_actors_children = actor_df \
    .join(film_actor_df, actor_df.actor_id == film_actor_df.actor_id) \
    .join(film_df, film_actor_df.film_id == film_df.film_id) \
    .join(film_category_df, film_df.film_id == film_category_df.film_id) \
    .join(category_df, film_category_df.category_id == category_df.category_id) \
    .filter(category_df.name == "Children") \
    .groupBy(actor_df.actor_id, actor_df.first_name, actor_df.last_name) \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(3)


# In[27]:


top_actors_children.show(truncate = False)


# In[28]:


# 6. Вывести города с количеством активных и неактивных клиентов
city_customers = city_df.join(address_df, city_df.city_id == address_df.city_id) \
    .join(customer_df, address_df.address_id == customer_df.address_id) \
    .groupBy(city_df.city.alias("city")) \
    .agg(
        sum(when(customer_df.active == 1, 1)).alias("active_customers"),
        sum(when(customer_df.active == 0, 1)).alias("inactive_customers")
    ) \
    .orderBy("inactive_customers", ascending=False)


# In[29]:


city_customers.show(600, truncate = False)


# In[30]:


# 7. Вывести категорию фильмов с самым большим количеством часов суммарной аренды в городах, начинающихся на "a"
#    и содержащих символ "-"

query = """
SELECT c.name,
       CAST((EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600) AS INT) AS rental_duration_in_hours,
       c3.city
FROM category c
INNER JOIN film_category fc ON c.category_id = fc.category_id
INNER JOIN film f ON fc.film_id = f.film_id
INNER JOIN inventory i ON f.film_id = i.film_id
INNER JOIN rental r ON i.inventory_id = r.inventory_id
INNER JOIN customer c2 ON r.customer_id = c2.customer_id
INNER JOIN address a ON c2.address_id = a.address_id
INNER JOIN city c3 ON a.city_id = c3.city_id
WHERE (r.return_date IS NOT NULL AND r.rental_date IS NOT NULL
       AND c3.city LIKE 'a%')
      OR (r.return_date IS NOT NULL AND r.rental_date IS NOT NULL
          AND c3.city LIKE '%-%')
ORDER BY rental_duration_in_hours DESC
"""

film_cat_filtered = spark.read.jdbc(url, "(%s) AS tmp" % query, properties=properties)


# In[31]:


film_cat_filtered.show(truncate = False)


# In[32]:


film_cat_filtered_2 = category_df \
    .join(film_category_df, category_df.category_id == film_category_df.category_id) \
    .join(film_df, film_category_df.film_id == film_df.film_id) \
    .join(inventory_df, film_df.film_id == inventory_df.film_id) \
    .join(rental_df, inventory_df.inventory_id == rental_df.inventory_id) \
    .join(customer_df, rental_df.customer_id == customer_df.customer_id) \
    .join(address_df, customer_df.address_id == address_df.address_id) \
    .join(city_df, address_df.city_id == city_df.city_id) \
    .filter((rental_df.return_date.isNotNull()) & (rental_df.rental_date.isNotNull()) &
            ((city_df.city.like('a%')) | (city_df.city.like('%-%')))) \
    .select(category_df.name.alias('name'),
            round((unix_timestamp(rental_df.return_date) - unix_timestamp(rental_df.rental_date)) / 3600)
            .alias('rental_duration_in_hours'),
            city_df.city) \
    .orderBy(expr('rental_duration_in_hours').desc())


# In[33]:


film_cat_filtered_2.show()


# In[34]:


SparkSession.stop(spark)

