from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def product_category_pairs(products: DataFrame,
                           categories: DataFrame,
                           product_categories: DataFrame) -> DataFrame:
    """
    Возвращает DataFrame со всеми парами "product_name - category_name",
    а также продукты без категорий.
    
    products: DataFrame с колонками ['product_id', 'product_name']
    categories: DataFrame с колонками ['category_id', 'category_name']
    product_categories: DataFrame с колонками ['product_id', 'category_id']
    
    Возвращает DataFrame с колонками ['product_name', 'category_name']
    """
    # джоин products и product_categories
    prod_cat = products.join(product_categories, on="product_id", how="left") \
                       .join(categories, on="category_id", how="left") \
                       .select(col("product_name"), col("category_name"))
    
    return prod_cat
