import setuptools
import os

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

version = '0.0.0'
if os.path.exists('version.txt'):
    with open('version.txt') as f:
        version = f.read()

    
setuptools.setup(
    name="co-connect-tools", 
    author="CO-CONNECT",
    version=version,
    author_email="CO-CONNECT@dundee.ac.uk",
    description="Python package for performing mapping of ETL to CDM ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CO-CONNECT/co-connect-tools",
    scripts = ['scripts/etlcdm.py'],
    entry_points = {
        'console_scripts':[
            'etl2cdm=coconnect.cli.etl2cdm:main',
            'coconnect=coconnect.cli.cli:coconnect',
            #'process_rules=coconnect.cli.process_rules:main',
        ],
    },
    packages=setuptools.find_packages(),
    install_requires=required,
    package_data={'coconnect': ['data/cdm/*','data/example/*/*']},
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
