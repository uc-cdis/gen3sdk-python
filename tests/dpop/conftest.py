"""
Values shared by the DPoP tests: the fake commons and the token it hands out.

Both the proxy tests and the Nextflow tests assert on these, so they live here to
keep the two files describing the same commons.
"""

TASK_TOKEN = "test-task-token-abc123"
COMMONS = "https://gen3.example.com"
