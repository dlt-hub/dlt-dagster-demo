from setuptools import find_packages, setup

setup(
    name="github_issues",
    packages=find_packages(exclude=["github_issues_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
