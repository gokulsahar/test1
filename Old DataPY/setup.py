# setup.py
from setuptools import setup, find_packages

setup(
    name="datapy",
    version="0.1.0",
    description="CLI-first, YAML-driven ETL framework",
    author="DataPY Team",
    packages=find_packages(include=["pype", "pype.*"]),
    install_requires=[
        "ruamel.yaml",
        "pydantic",
        "click",
        "networkx", 
        "pandas>=2.0.0",
        "numpy",
        "dask[complete]>=2023.1.0",
        "msgpack",
        "requests",
        "typing_extensions",
        "toolz",
        "cloudpickle",
        "partd",
        "fsspec",
        "python-dateutil",
        "tzdata",
        "pytz",
        "jsonschema",
        "graphviz",
    ],
    entry_points={
        "console_scripts": [
            "pype = pype.cli.cli:cli",
        ],
    },
)
