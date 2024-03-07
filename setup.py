from setuptools import setup, find_packages

import os
if os.path.isfile('requirements.txt'):
    with open('requirements.txt', 'r') as f:
        install_requires = f.read().splitlines()
else:
    install_requires = ''

_VERSION = '0.1'
print(_VERSION)

setup(
    name='paeio',
    version=_VERSION,
    description='In and out operations for Azure Cloud',
    author='Joao Pedro Alves and Mauricio Araujo',
    author_email='jotapedro1997@gmail.com',
    install_requires=install_requires,
    license='MIT',
    include_package_data=True,
    packages=find_packages(),
    python_requires=">=3.7.1",
    zip_safe=False
)