"""Per-subcommand modules.

Each module exposes a single ``register(group: click.Group)`` function that
the top-level ``cli`` group calls during startup.
"""
