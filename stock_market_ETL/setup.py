from setuptools import find_packages, setup

setup(
    name="stock_market_ETL",
    packages=find_packages(exclude=["stock_market_ETL_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
