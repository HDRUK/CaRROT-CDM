import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()
    
setuptools.setup(
    name="co-connect", 
    version="0.0.1",
    author="CO-CONNECT",
    author_email="CO-CONNECT@dundee.ac.uk",
    description="Python package for performing mapping of ETL to CDM ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CO-CONNECT/etl-tools",
    packages=setuptools.find_packages(),
    install_requires=required,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)
