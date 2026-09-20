"""Errors shared by the consumers."""


class TransientError(Exception):
    """A failure that will pass by itself (database, DLQ or schema registry down):
    retry with backoff, never commit, never dead-letter."""


class Shutdown(Exception):
    """A stop was requested while retrying: leave WITHOUT committing (safe: the
    batch is simply re-read)."""
