#!/usr/bin/env python

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='lw2bacnet',
    version='1.0.0',
    description='LoRaWAN to BACnet Bridge',
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        'console_scripts': ['lw2bacnet=lw2bacnet.lw2bacnet:main']
    },
    packages=find_packages(include=['lw2bacnet', 'lw2bacnet.*']),
    install_requires=[
        'attrs==22.2.0',
        'BAC0==22.9.21',
        'bacpypes==0.18.6',
        'colorama==0.4.6',
        'exceptiongroup==1.1.0',
        'flatdict==4.0.1',
        'iniconfig==2.0.0',
        'packaging==23.0',
        'paho-mqtt==1.6.1',
        'pluggy==1.0.0',
        'pytz==2022.7.1',
        'PyYAML==6.0.1',
        'quickjs==1.19.2',
        'tomli==2.0.1',
    ],
    classifiers=[
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Utilities',
    ],
)
