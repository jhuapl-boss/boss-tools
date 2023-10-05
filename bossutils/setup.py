from setuptools import setup, find_packages
import os
import pathlib

here = pathlib.Path(__file__).parent.resolve()

with open(os.path.join(here, "requirements.txt"), encoding="utf-8") as f:
    reqs = f.read().split("\n")
requirements = [r.strip() for r in reqs]

setup(
    name="bossutils",
    version='0.1.0',
    author="APL",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.6, <4",
    install_requires=requirements,
    package_data={"bossutils": ["lambda_logger_conf.json", "logger_conf.json", "logger.conf.orig"]},
)
