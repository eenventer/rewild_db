from setuptools import find_packages, setup

setup(
    name="rewild_dagster",
    packages=find_packages(exclude=["rewild_dagster_tests"]),
    install_requires=[
        "dagster",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
