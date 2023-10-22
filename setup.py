from setuptools import find_packages, setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="p2uc",
    version="0.0.1",
    description="Import metadata from Azure Purview into Databricks Unity Catalog",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vikasgautam-db/purview-to-uc",
    author="Vikas Gautam",
    author_email="vikas.gautam@databricks.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    install_requires=["pyapacheatlas >= 0.15.0", "databricks-sdk >= 0.11.0"],
    python_requires=">=3.8",
)