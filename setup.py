from setuptools import setup, find_packages


with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='scicat_ingest',
    version='0.0.1',
    url='https://github.com/als-computing/scicat_ingest.git',
    author='Dylan McReynolds',
    author_email='dmcreynolds@lbl.gov',
    description='Communication code for scicat',
    packages=find_packages(),    
    install_requires=required
)