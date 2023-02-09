from setuptools import find_packages, setup

__version__ = "1.0.0"

setup(
    name="airflow-provider-bigquery-commitment",
    version=__version__,
    description="An Apache Airflow provider concerning BigQuery slots commitment.",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=bigquery_commitment.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=find_packages(exclude=["*tests.*", "*tests"]),
    install_requires=[
        "apache-airflow>=2.3",
        "google-cloud-bigquery-reservation==1.8.0",
    ],
    setup_requires=["setuptools", "wheel"],
    author="Pierre Cardona",
    author_email="pierre@data-fullstack.com",
    url="https://data-fullstack.com/",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires="~=3.8",
)
