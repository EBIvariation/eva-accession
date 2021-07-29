import os
from setuptools import find_packages, setup


def get_requires():
    requires = []
    with open(os.path.join(os.path.dirname(__file__), "requirements.txt"), "rt") as req_file:
        for line in req_file:
            requires.append(line.rstrip())
    return requires


setup(name='clustering_automation',
      version='0.0.1',
      packages=find_packages(),
      install_requires=get_requires(),
      tests_require=get_requires(),
      setup_requires=get_requires()
)
