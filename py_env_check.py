pkgs = [
    "ruamel.yaml", "pydantic", "click", "networkx", "pandas", "numpy", "dask",
    "msgpack", "requests", "typing_extensions", "toolz", "cloudpickle",
    "partd", "fsspec", "python-dateutil", "tzdata", "pytz", "jsonschema", "graphviz",
    "pytest", "pytest_cov", "black", "isort", "mypy", "pre_commit",
]

missing = []
for mod in pkgs:
    try:
        __import__(mod.replace("-", "_"))
    except ImportError:
        missing.append(mod)

if not missing:
    print("All DataPY packages present!")
else:
    print("Missing:", ", ".join(missing))
