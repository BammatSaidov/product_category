#Product Category PySpark Library

Библиотека для PySpark, возвращающая все пары «Продукт – Категория» и продукты без категорий.
Позволяет обрабатывать продукты, которые могут иметь несколько категорий или не иметь ни одной.

#Установка
pip install -e .

#Пример использования
from pyspark.sql import SparkSession
from product_category.product_category_pairs import product_category_pairs

spark = SparkSession.builder.master("local[*]").appName("example").getOrCreate()

products = spark.createDataFrame([
    (1, "Apple"),
    (2, "Banana"),
    (3, "Carrot")
], ["product_id", "product_name"])

categories = spark.createDataFrame([
    (1, "Fruit"),
    (2, "Vegetable")
], ["category_id", "category_name"])

product_categories = spark.createDataFrame([
    (1, 1),
    (2, 1)
], ["product_id", "category_id"])

result = product_category_pairs(products, categories, product_categories)
result.show()

#Тесты
pytest tests/
