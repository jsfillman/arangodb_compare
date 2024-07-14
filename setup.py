from setuptools import setup, find_packages

setup(
    name="arangodb_compare",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "arango",
    ],
    entry_points={
        'console_scripts': [
            'arangodb_compare=arangodb_compare.main:main',
        ],
    },
)

