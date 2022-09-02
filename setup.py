from setuptools import setup

setup(
    name="subgraph_extractor",
    version="0.2.0",
    description="Pull data from graph-node databases into parquet files",
    long_description="Pull data from graph-node databases into parquet files with hierarchal partitioning",
    long_description_content_type="text/plain",
    url="http://github.com/cardstack/subgraph-extractor",
    author="Ian Calvert",
    author_email="ian.calvert@cardstack.com",
    license="None",
    install_requires=[
        "click",
        "pandas",
        "pyyaml",
        "sqlalchemy",
        "psycopg2-binary",
        "pyarrow>=9",
        "simple-term-menu",
        "tqdm",
        "cloudpathlib[s3]>=0.9.0",
        "deepdiff",
    ],
    extras_require={
        "dev": [
            "pytest",
            "black",
            "isort",
            "pytest-pep8",
            "pytest-cov",
            "pytest-postgresql",
            "hypothesis",
        ]
    },
    entry_points={
        "console_scripts": [
            "subgraph_extractor=subgraph_extractor.cli:main",
            "subgraph_config_generator=subgraph_extractor.cli:config_generator",
        ],
    },
    packages=["subgraph_extractor"],
    zip_safe=False,
)
