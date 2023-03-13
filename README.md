# Gen3 SDK for Python (w/ CLI)

The Gen3 Software Development Kit (SDK) for Python provides classes and functions for handling common tasks when interacting with a Gen3 commons. It also exposes a Command Line Interface (CLI).

The API for a commons can be overwhelming, so this SDK/CLI aims
to simplify communication with various microservices.

The docs here contain general descriptions of the different pieces of the SDK and example scripts. For detailed API documentation, see the link below:

* [Detailed API Documentation](https://uc-cdis.github.io/gen3sdk-python/_build/html/index.html)


## Table of Contents

- [Gen3 SDK for Python](#gen3-sdk-for-python)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Scripting Quickstart](docs/tutorial/quickStart.md)
  - [Available Classes](docs/reference/sdkClasses.md)
  - [Indexing Tools](docs/howto/diirmIndexing.md)
  - [Metadata Tools](docs/howto/metadataTools.md)
    - [Gen3 Discovery Page Metadata Tools](docs/howto/discoveryMetadataTools.md)
    - [Gen3 Subject-level Crosswalk Metadata Tools](docs/howto/crosswalk.md)
  - [Bundle Tools](docs/howto/bundleTools.md)
  - [Development](docs/howto/devTest.md)

---

## Installation

To get the latest released version of the SDK:

`pip install gen3`

This SDK exposes a Command Line Interface (CLI). You can now import functions from `gen3` into your own Python scripts or you can use the command line interface:

`gen3 --help`
