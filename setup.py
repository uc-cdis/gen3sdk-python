from setuptools import setup, find_packages
from subprocess import check_output


def get_version():
    try:
        tag = check_output(
            ["git", "describe", "--tags", "--abbrev=0", "--match=[0-9]*"]
        )
        return tag.decode("utf-8").strip("\n")
    except Exception:
        # if somehow you get the repo not from git,
        # hardcode default major version
        return "0.1.0"


with open("README.md", "r") as f:
    README = f.read()

with open("CHANGELOG.md", "r") as f:
    CHANGELOG = f.read()

setup(
    name="gen3",
    description=(
        "The Gen3 SDK makes it easy to utilize " "functionality in Gen3 data commons. "
    ),
    long_description=README + "\n\n" + CHANGELOG,
    long_description_content_type="text/markdown",
    author="Center for Translational Data Science",
    author_email="support@datacommons.io",
    license="Apache 2.0",
    url="https://gen3.org/",
    version=get_version(),
    packages=find_packages(),
    install_requires=[
        "requests",
        "pandas",
        "indexclient",
        "aiohttp",
        "backoff",
        "click",
    ],
    dependency_links=[
        "git+https://github.com/uc-cdis/indexclient.git@1.6.2#egg=indexclient"
    ],
    package_data={"": ["LICENSE"]},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
        "Topic :: Scientific/Engineering",
    ],
)
