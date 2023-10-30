from setuptools import find_packages, setup

setup(
    name="mongodb_dlt",
    packages=find_packages(exclude=["mongodb_dlt_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
