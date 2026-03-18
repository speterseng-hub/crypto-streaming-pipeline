"""
setup.py — needed so Dataflow workers can install this package.
"""
import setuptools

setuptools.setup(
    name="crypto-streaming-pipeline",
    version="0.1.0",
    packages=setuptools.find_packages(),
    install_requires=[
        "apache-beam[gcp]==2.60.0",
        "google-cloud-pubsub==2.23.0",
        "google-cloud-bigquery==3.25.0",
        "google-cloud-storage==2.18.0",
        "google-cloud-bigtable==2.26.0",
        "pydantic==2.9.2",
        "structlog==24.4.0",
    ],
)
