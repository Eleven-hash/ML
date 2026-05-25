"""
Flight Analytics Platform - Setup Configuration.
"""

from setuptools import setup, find_packages

setup(
    name="flight-analytics-platform",
    version="1.0.0",
    description="Real-Time Flight Analytics Platform using Databricks & PySpark",
    author="Flight Analytics Team",
    python_requires=">=3.9",
    packages=find_packages(exclude=["tests*", "notebooks*"]),
    install_requires=[
        "pyspark>=3.4.0",
        "requests>=2.31.0",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
        ],
        "kafka": [
            "confluent-kafka>=2.3.0",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
