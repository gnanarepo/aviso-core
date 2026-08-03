"""Methods injected into the shell that connects to this service.

Empty on purpose, as in aggregator-service and sales-engagement: callers reach
the endpoints through ``shell.api('/gbm/...')``. Named methods land here as the
remaining GBM APIs are migrated.
"""
shell_methods = {}
