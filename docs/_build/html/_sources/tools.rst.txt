Gen3 Tools
----------

Tools and functions for common actions in Gen3. These tools are broken up into broad categories like indexing (for tasks related to the file object persistent identifiers within the system) and metadata (for tasks relating to file object metadata within the system).

Such common indexing tasks may involve indexing file object URLs into Gen3 to assign persistent identifiers, downloading a manifest of every file object that already exists, and verifying that a Gen3 instance contains the expected indexed file objects based on a file.

For metadata, the task may be ingesting a large amount of metadata from a file into the system.

Most of these tools utilize async capabilities of Python to make common tasks more efficient.

.. toctree::
    :glob:

    tools/*

.. automodule:: gen3.tools
   :members:
   :show-inheritance:
