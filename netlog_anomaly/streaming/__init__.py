"""Streaming mode: replay the log through Kafka, window it, score it, index it.

The batch pipeline in the parent package aggregates windows with one SQL
GROUP BY over a table that already holds every row. A stream has no such
table: rows arrive one at a time, out of order, and a window has to be closed
on evidence rather than on reaching the end of the input. That evidence is the
watermark in `windows`, and everything else here is built on it.
"""
