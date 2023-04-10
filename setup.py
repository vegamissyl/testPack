from setuptools import setup

setup(name="testPack",
version="0.0.1",
description="test packaging",
author="jeo",
author_email="jeomen@outlook.com",
packages=["src","test"],
install_requires=["py4j", "pandas","pyspark"],
license="Apache 2.0",
include_package_data=True,
package_data={'': ['*.csv']})