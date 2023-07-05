"""
gen3.external

For housing clients and wrappers against non-Gen3 APIs. At the moment, they
are usually used for gathering metadata.

If you're adding a new metadata source, use the ExternalMetadataSourceInterface.
"""
from gen3.external.external import ExternalMetadataSourceInterface
