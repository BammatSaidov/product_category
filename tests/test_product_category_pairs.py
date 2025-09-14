import pytest
from pyspark.sql import SparkSession
from product_category.product_category_pairs import product_category_pairs

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()

def test_product_category_pairs(spark):
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
        (1, 1),  # Apple -> Fruit
        (2, 1)   # Banana -> Fruit
    ], ["product_id", "category_id"])

    expected = [
        ("Apple", "Fruit"),
        ("Banana", "Fruit"),
        ("Carrot", None)
    ]

    df_result = product_category_pairs(products, categories, product_categories)
    actual = [(row["product_name"], row["category_name"]) for row in df_result.collect()]

    assert sorted(actual) == sorted(expected)
