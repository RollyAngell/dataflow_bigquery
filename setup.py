"""Setup script for the Dataflow BigQuery pipeline."""

from setuptools import find_packages, setup

with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = [
        line.strip() for line in f if line.strip() and not line.startswith("#")
    ]

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dataflow-bigquery-pipeline",
    version="1.0.0",
    description="Batch pipeline for processing CSV files from GCS to BigQuery using Dataflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Data Engineering Team",
    author_email="data-engineering@example.com",
    url="https://github.com/your-org/dataflow-bigquery-pipeline",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "flake8>=7.0.0",
            "black>=24.1.0",
            "isort>=5.13.0",
            "mypy>=1.8.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "run-pipeline=pipeline.main:run",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
