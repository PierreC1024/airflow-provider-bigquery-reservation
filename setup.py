from setuptools import find_packages, setup

__version__ = "0.0.1"

setup(
    name="airflow-provider-bigquery-reservation",
    version=__version__,
    description="An Apache Airflow provider concerning BigQuery reservation.",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=airflow_provider_bigquery_reservation.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=find_packages(exclude=["*tests.*", "*tests"]),
    install_requires=[
        "apache-airflow>=2.5.0",
        "google-cloud-bigquery-reservation==1.8.0",
    ],
    setup_requires=["setuptools", "wheel"],
    author="Pierre Cardona",
    author_email="pierre@data-fullstack.com",
    url="https://github.com/PierreC1024/airflow-provider-bigquery-reservation",
    download_url="https://github.com/PierreC1024/airflow-provider-bigquery-reservation/archive/refs/tags/0.0.1.tar.gz",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires="~=3.8",
    keywords = ["airflow", "bigquery"],
)
