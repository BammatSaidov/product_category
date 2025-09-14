from setuptools import setup, find_packages

setup(
    name="product_category",
    version="0.1.0",
    description="PySpark library to get product-category pairs",
    packages=find_packages(),
    install_requires=["pyspark>=3.4.0"],
)
