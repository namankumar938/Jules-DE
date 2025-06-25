import logging
import sys

def setup_logging(level=logging.INFO, name: str | None = None):
    """
    Configures logging for the application.

    If 'name' is None, configures the root logger.
    Otherwise, configures the logger with the given name.
    Adds a formatted console handler. Prevents adding duplicate handlers
    to the specified logger if it already has them.

    Args:
        level: The logging level to set (e.g., logging.INFO, logging.DEBUG).
        name: Optional name of the logger to configure. If None, configures root logger.
    """
    # Get the specific logger or the root logger
    logger_to_configure = logging.getLogger(name)

    # Check if this specific logger already has handlers.
    # This is more targeted than checking logging.getLogger().hasHandlers() if name is not None.
    if logger_to_configure.hasHandlers() and name is not None:
        # If configuring a specific named logger and it already has handlers, assume it's configured.
        # For root logger, it's common to check and add only if no handlers exist at all on root.
        # Or, if we always want THIS handler, we might clear existing handlers first (more aggressive).
        # For this project, let's assume configuring the root once is the goal for the main script.
        # For library-like usage, configuring getLogger(__name__) is better without touching root.
        # Given the task asks to check logging.getLogger().hasHandlers(), this implies root logger focus.
        pass

    # If configuring root logger specifically, or if no name is given (defaults to root)
    # only add handlers if the root logger itself has no handlers.
    # This is the primary interpretation of "if not logging.getLogger().hasHandlers():"
    if name is None and logging.getLogger().hasHandlers():
        # Root logger already configured, do nothing further to avoid duplication on root.
        # print(f"Root logger already has handlers. Skipping setup_logging for root.", file=sys.stderr)
        return

    logger_to_configure.setLevel(level)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout) # Use sys.stdout for consistency
    console_handler.setLevel(level) # Handler level can also be set

    # Create formatter and add it to the handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger_to_configure.addHandler(console_handler)

    # If we configured a specific logger, ensure its messages are not propagated to the root
    # if the root also gets its own handler by a separate call.
    # However, for this project, we'll likely call setup_logging() once for the root from main.
    # If other modules call setup_logging() for their own named loggers, this is fine.
    # If they call setup_logging() with name=None, the check above should prevent re-configuring root.
    # if name is not None:
    # logger_to_configure.propagate = False # Optional: if this logger should not pass messages to root

if __name__ == '__main__':
    # Example usage and test
    setup_logging(level=logging.DEBUG) # Configure root logger for this test

    # Test root logger
    logging.info("This is an INFO message from root logger.")
    logging.debug("This is a DEBUG message from root logger.")

    # Test module-level logger (will inherit from root if not separately configured)
    module_logger = logging.getLogger("MyModuleTest")
    # If setup_logging was called for "MyModuleTest", it would have its own handlers.
    # If not, it sends to root. Let's test it as if it's a typical module logger.
    # To make it use the root config for this test, we don't call setup_logging("MyModuleTest")
    module_logger.info("This is an INFO message from MyModuleTest logger.")
    module_logger.debug("This is a DEBUG message from MyModuleTest logger (should appear if root is DEBUG).")

    # Test another logger to ensure no interference if setup_logging is specific
    another_logger = logging.getLogger("AnotherModule")
    another_logger.warning("Warning from AnotherModule.")

    # Test re-calling for root (should ideally not add more handlers to root)
    print("\nRe-calling setup_logging for root (should not duplicate handlers on root):")
    setup_logging(level=logging.INFO) # Attempt to reconfigure root
    logging.info("INFO message after re-calling setup_logging for root.")

    # For a truly isolated named logger test:
    # setup_logging(level=logging.DEBUG, name="NamedLoggerTest")
    # named_logger = logging.getLogger("NamedLoggerTest")
    # named_logger.info("Info from NamedLoggerTest")
    # named_logger.propagate = False # To show it's isolated
    # logging.info("This root message should still appear if root is configured.")
