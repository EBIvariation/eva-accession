import os
from setuptools import find_packages, setup


def get_requires():
    requires = []
    with open(os.path.join(os.path.dirname(__file__), "run_release_in_embassy", "requirements.txt"), "rt") as req_file:
        for line in req_file:
            requires.append(line.rstrip())
    with open(os.path.join(os.path.dirname(__file__), "gather_clustering_counts", "requirements.txt"), "rt") as req_file:
        for line in req_file:
            requires.append(line.rstrip())
    return requires


setup(name='run_release_in_embassy',
      version='0.0.1',
      packages=find_packages(),
      install_requires=get_requires(),
      tests_require=get_requires(),
      setup_requires=get_requires()
)
