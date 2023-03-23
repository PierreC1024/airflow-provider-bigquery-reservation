"""Package setup."""
from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

__version__ = "1.1.0"

setup(
    name="airflow-provider-bigquery-reservation",
    version=__version__,
    long_description=long_description,
    description="An Apache Airflow provider concerning BigQuery reservation.",
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=airflow_provider_bigquery_reservation.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=find_packages(exclude=["*tests.*", "*tests"]),
    install_requires=[
        "apache-airflow>=2.3.0",
        "google-cloud-bigquery-reservation>=1.0.0",
        "google-cloud-bigquery>=2.0.0",
    ],
    setup_requires=["setuptools", "wheel"],
    author="Pierre Cardona",
    author_email="pierre@data-fullstack.com",
    url="https://github.com/PierreC1024/airflow-provider-bigquery-reservation",
    download_url="https://github.com/PierreC1024/airflow-provider-bigquery-reservation/archive/refs/tags/0.2.0.tar.gz",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires=">=3.7",
    keywords=["airflow", "bigquery"],
)
