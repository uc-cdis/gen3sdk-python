import asyncio
import collections.abc
from jsonschema import Draft4Validator
import sys
import re
import requests
import random
import string
import os

from urllib.parse import urlunsplit
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.parse import parse_qs

from cdislogging import get_logger

logging = get_logger("__name__")

UUID_FORMAT = (
    r"^.*[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
)
MD5_FORMAT = r"^[a-fA-F0-9]{32}$"
SIZE_FORMAT = r"^[0-9]*$"
ACL_FORMAT = r"^.*$"
URL_FORMAT = r"^.*$"
AUTHZ_FORMAT = r"^.*$"


def get_random_alphanumeric(length):
    # end up with roughly the same amount of numbers as letters
    letters = string.ascii_lowercase + "".join([str(item) for item in range(0, 10)]) * 3
    return "".join(random.choice(letters) for i in range(length))


def make_folders_for_filename(filename, current_directory=None):
    """
    Make the directories up to the filename provided. Relative paths are supported.
    Ensure you supply the current directory you want relative paths to be
    based off of. Returns the absolute path for the file with the folders
    created. This does NOT create the actual file.

    Example:
        output = make_folders_for_filename(
            "../test/temp/output.txt",
            current_directory="/home/me/foobar"
        )
        print(output)
        >>> /home/me/test/temp/output.txt

    Args:
        filename (str): path to desired file
        current_directory (str, optional): current directory you want relative paths to be
    based off of

    Returns:
        str: the absolute path for the file with the folders created
    """
    current_directory = os.path.dirname(os.path.realpath(__file__))
    directory = os.path.dirname(os.path.abspath(filename))
    if not os.path.exists(directory):
        os.makedirs(directory)

    absolute_path_filename = (
        directory.rstrip("/") + "/" + os.path.abspath(filename).split("/")[-1]
    )

    return absolute_path_filename


def get_or_create_event_loop_for_thread():
    """
    Asyncio helper function to attempt to get a currently running loop and
    if there isn't one in the thread, create one and set it so future calls
    get the same event loop.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # no loop for this thread, so create one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    except AttributeError:
        # handle older versions of asyncio for previous versions of Python,
        # specifically this allows Python 3.6 asyncio to get a loop
        loop = asyncio._get_running_loop()
        if not loop:
            loop = asyncio.get_event_loop()

    return loop


def raise_for_status_and_print_error(response):
    """
    This only works for sync requests
    """
    try:
        response.raise_for_status()
    except requests.HTTPError as exception:
        print(
            "Error: status code {}; details:\n{}".format(
                response.status_code, response.text
            )
        )
        raise


def append_query_params(original_url, **kwargs):
    """
    Add additional query string arguments to the given url.

    Example call:
        new_url = append_query_params(
            original_url, error='this is an error',
            another_arg='this is another argument')
    """
    scheme, netloc, path, query_string, fragment = urlsplit(original_url)
    query_params = parse_qs(query_string)
    if kwargs is not None:
        for key, value in kwargs.items():
            query_params[key] = [value]

    new_query_string = urlencode(query_params, doseq=True)
    new_url = urlunsplit((scheme, netloc, path, new_query_string, fragment))
    return new_url


def split_url_and_query_params(url):
    """
    Given a url, return the url (no query params) and the split out the
    query paramaters separately
    """
    scheme, netloc, path, query_string, fragment = urlsplit(url)
    query_params = parse_qs(query_string)
    url = urlunsplit((scheme, netloc, path, None, fragment))
    return url, query_params


def remove_trailing_whitespace_and_slashes_in_url(url):
    """
    Given a url, remove any whitespace and then slashes at the end and return url
    """
    if url:
        return url.rstrip().rstrip("/")

    return url


def _print_func_name(function):
    return "{}.{}".format(function.__module__, function.__name__)


def _print_kwargs(kwargs):
    return ", ".join("{}={}".format(k, repr(v)) for k, v in list(kwargs.items()))


def log_backoff_retry(details):
    args_str = ", ".join(map(str, details["args"]))
    kwargs_str = (
        (", " + _print_kwargs(details["kwargs"])) if details.get("kwargs") else ""
    )
    func_call_log = "{}({}{})".format(
        _print_func_name(details["target"]), args_str, kwargs_str
    )
    logging.warning(
        "backoff: call {func_call} delay {wait:0.1f} seconds after {tries} tries".format(
            func_call=func_call_log, **details
        )
    )


def log_backoff_giveup(details):
    args_str = ", ".join(map(str, details["args"]))
    kwargs_str = (
        (", " + _print_kwargs(details["kwargs"])) if details.get("kwargs") else ""
    )
    func_call_log = "{}({}{})".format(
        _print_func_name(details["target"]), args_str, kwargs_str
    )
    logging.error(
        "backoff: gave up call {func_call} after {tries} tries; exception: {exc}".format(
            func_call=func_call_log, exc=sys.exc_info(), **details
        )
    )


def log_backoff_giveup_except_on_no_retries(details):
    args_str = ", ".join(map(str, details["args"]))
    kwargs_str = (
        (", " + _print_kwargs(details["kwargs"])) if details.get("kwargs") else ""
    )
    func_call_log = "{}({}{})".format(
        _print_func_name(details["target"]), args_str, kwargs_str
    )

    if details["tries"] > 1:
        logging.error(
            "backoff: gave up call {func_call} after {tries} tries; exception: {exc}".format(
                func_call=func_call_log, exc=sys.exc_info(), **details
            )
        )


def get_delimiter_from_extension(filename):
    """
    Return the file delimter based on the extension.

    Args:
        filename (str): file name with extension

    Returns:
        str: delimeter character, either \t or ,
    """
    file_ext = os.path.splitext(filename)
    if file_ext[-1].lower() == ".tsv":
        file_delimiter = "\t"
    else:
        # default, assume CSV
        file_delimiter = ","
    return file_delimiter


def is_status_code(error, code):
    """
    Args:
        error (object): Ideally a requests.Response, this safely checks for
            known attributes where the status code might be
        code (str): The status code you want to check for in the error. ex: 404

    Returns:
        bool: Whether or not the error object contains the status code specified
    """
    code = str(code)
    return (
        str(getattr(error, "code", None)) == code
        or str(getattr(error, "status", None)) == code
        or str(getattr(error, "status_code", None)) == code
        or str(getattr(getattr(error, "response", {}), "code", "")) == code
        or str(getattr(getattr(error, "response", {}), "status", "")) == code
        or str(getattr(getattr(error, "response", {}), "status_code", "")) == code
    )


def exception_do_not_retry(error):
    if is_status_code(error, "409") or is_status_code(error, "404"):
        return True

    return False


def _verify_format(s, format):
    """
    Make sure the input is in the right format
    """
    r = re.compile(format)
    if r.match(s) is not None:
        return True
    return False


def _verify_schema(data, schema):
    validator = Draft4Validator(schema)
    validator.iter_errors(data)
    errors = [e.message for e in validator.iter_errors(data)]
    if errors:
        logging.error(
            f"Error validating package contents {data} against schema {schema}. Details: {errors}"
        )
        return False
    return True


def _standardize_str(s):
    """
    Remove unnecessary spaces

    Ex. "abc    d" -> "abc d"
    """
    memory = []
    res = ""
    for c in s:
        if c != " ":
            res += c
            memory = []
        elif not memory:
            res += c
            memory.append(" ")
    return res


def get_urls(raw_urls_string):
    """
    Given raw string like "['gs://topmed-irc-share/genomes/NWD293573%20file.b38.irc.v1.cram', 's3://nih-nhlbi-datacommons/NWD293573.b38.irc.v1.cram']"
    return a list of urls:

    [
        "gs://topmed-irc-share/genomes/NWD293573 file.b38.irc.v1.cram",
        "s3://nih-nhlbi-datacommons/NWD293573.b38.irc.v1.cram"
    ]
    """
    return [
        element.strip()
        .replace("'", "")
        .replace('"', "")
        .replace("%20", " ")
        .rstrip(",")
        for element in _standardize_str(raw_urls_string)
        .strip()
        .lstrip("[")
        .rstrip("]")
        .split(" ")
    ]


def yield_chunks(input_list, n):
    """
    Yield successive n-sized chunks from input_list.

    Args:
        input_list (list[]): arbitrary input list
        n (int): size of chunks requested

    Yields:
        list[]: chunked list
    """
    for i in range(0, len(input_list), n):
        yield input_list[i : i + n]


def deep_dict_update(a, b):
    """
    a is updated in place to include items in b

    This recursively handles nested dictionary updates
    """
    for key, value in b.items():
        if isinstance(value, collections.abc.Mapping):
            a[key] = deep_dict_update(a.get(key, {}), value)
        else:
            a[key] = value
    return a


# Default settings to control usage of backoff library.
DEFAULT_BACKOFF_SETTINGS = {
    # Disable backoff lib default logger, only show custom logs
    "logger": None,
    "on_backoff": log_backoff_retry,
    "on_giveup": log_backoff_giveup,
    "max_tries": int(os.environ.get("GEN3SDK_MAX_RETRIES", 3)),
    "giveup": exception_do_not_retry,
}

# Metadata.get settings to control usage of backoff library.
BACKOFF_NO_LOG_IF_NOT_RETRIED = {
    # Disable backoff lib default logger, only show custom logs
    "logger": None,
    "on_backoff": log_backoff_retry,
    "on_giveup": log_backoff_giveup_except_on_no_retries,
    "max_tries": int(os.environ.get("GEN3SDK_MAX_RETRIES", 3)),
    "giveup": exception_do_not_retry,
}
