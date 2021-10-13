from setuptools import setup

setup(
    name="subgraph_extractor",
    version="0.0.1",
    description="Pull data from graph-node databases into parquet files",
    url="http://github.com/cardstack/subgraph-extractor",
    author="Ian Calvert",
    author_email="ian.calvert@cardstack.com",
    license="None",
    install_requires=["click", "pandas", "sqlalchemy", "psycopg2-binary", "pyarrow"],
    extras_require={"dev": ["pytest", "black", "isort", "pytest-pep8", "pytest-cov"]},
    entry_points={
        "console_scripts": ["subgraph_extractor=subgraph_extractor.cli:main"],
    },
    packages=["subgraph_extractor"],
    zip_safe=False,
)
