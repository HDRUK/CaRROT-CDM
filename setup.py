import setuptools
import os
import sys

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()
    print (long_description)
cwd = os.getcwd()
os.system(f'ls {cwd}')
with open('requirements.txt') as f:
    required = f.read().splitlines()


sys.path.append("carrot/")
from _version import __version__ as version

    
setuptools.setup(
    name="carrot-cdm", 
    author="CO-CONNECT Collaboration",
    version=version,
    author_email="calmacx@gmail.com",
    description="Python package for performing mapping of ETL to CDM ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/HDRUK/CaRROT-CDM",
    entry_points = {
        'console_scripts':[
            'carrot=carrot.cli.cli:carrot'
        ],
    },
    packages=setuptools.find_packages(),
    extras_require = {
        'airflow':['apache-airflow'],
        'performance':['snakeviz'],
    },
    install_requires=required,
    package_data={'carrot': ['data/cdm/*','data/example/*/*','data/test/*/*','data/test/*/*/*']},
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
