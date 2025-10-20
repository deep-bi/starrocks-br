import click


def info(message: str) -> None:
    """Log an informational message."""
    click.echo(message)


def success(message: str) -> None:
    """Log a success message with checkmark."""
    click.echo(f"✓ {message}")


def warning(message: str) -> None:
    """Log a warning message with warning symbol."""
    click.echo(f"⚠ {message}", err=True)


def error(message: str) -> None:
    """Log an error message with error prefix."""
    click.echo(f"Error: {message}", err=True)


def critical(message: str) -> None:
    """Log a critical error message with critical symbol."""
    click.echo(f"❌ CRITICAL: {message}", err=True)


def progress(message: str) -> None:
    """Log a progress message with hourglass symbol."""
    click.echo(f"⏳ {message}")


def tip(message: str) -> None:
    """Log a tip message with lightbulb symbol."""
    click.echo(f"💡 {message}", err=True)
