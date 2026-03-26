# Gen3 SDK for Python (w/ CLI)

The Gen3 Software Development Kit (SDK) for Python provides classes and functions for handling common tasks when interacting with a Gen3 commons. It also exposes a Command Line Interface (CLI).

The API for a commons can be overwhelming, so this SDK/CLI aims
to simplify communication with various microservices.

The docs here contain general descriptions of the different pieces of the SDK and example scripts. For detailed API documentation, see the link below:

* [Detailed API Documentation](https://uc-cdis.github.io/gen3sdk-python/_build/html/index.html)


## Prerequisites

This project is built with Python. Ensure you have Python 3.6 or later installed.

Other prerequisites include:
- [pip](https://pip.pypa.io/en/stable/)
- Access to a Gen3 commons.

## Installation Steps:

### Using pip
To install the latest released version of the SDK, run:

```
pip install gen3
```

This SDK exposes a Command Line Interface (CLI). You can import functions from `gen3` into your own Python scripts or use the CLI:

```
gen3 --help
```

## External Documents

Additional documentation for different components is available:

- [Scripting Quickstart](docs/tutorial/quickStart.md)
- [Available Classes](docs/reference/sdkClasses.md)
- [Indexing Tools](docs/howto/diirmIndexing.md)
- [Metadata Tools](docs/howto/metadataTools.md)
  - [Gen3 Discovery Page Metadata Tools](docs/howto/discoveryMetadataTools.md)
  - [Gen3 Subject-level Crosswalk Metadata Tools](docs/howto/crosswalk.md)
- [Bundle Tools](docs/howto/bundleTools.md)
- [Development](docs/howto/devTest.md)
- [CLI](docs/howto/cli.md)

## Help and Support

For FAQs and commonly encountered errors, consult the documentation or use the CLI `--help` option:

```
gen3 --help
```

If you encounter issues, raise them on the [Gen3 SDK GitHub Issues page](https://github.com/uc-cdis/gen3sdk-python/issues).
