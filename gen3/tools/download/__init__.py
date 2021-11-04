from gen3.tools.download.drs_download import list_files_in_workspace_manifest
from gen3.tools.download.drs_download import download_files_in_workspace_manifest
from logging import StreamHandler, Formatter, INFO
from cdiserrors import get_logger

# Setup custom logger to create a console/friendly message
logger = get_logger("download", log_level="warning")
console = StreamHandler()
console.setLevel(INFO)
formatter = Formatter("%(message)s")
console.setFormatter(formatter)
logger.addHandler(console)
