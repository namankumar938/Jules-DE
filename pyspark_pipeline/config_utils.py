import yaml
import os
import logging

logger = logging.getLogger(__name__)

def load_config(config_filename: str = "config.yaml") -> dict | None:
    """
    Loads configuration from a YAML file.

    The function expects the YAML file to be located in the project root directory.
    The project root is determined by going one level up from the directory
    containing this script (config_utils.py).

    Args:
        config_filename: The name of the configuration file.
                         Defaults to "config.yaml".

    Returns:
        A dictionary containing the configuration, or None if loading fails.
    """
    try:
        # Determine the project root directory (parent of the directory containing this script)
        # __file__ is the path to config_utils.py (e.g., /app/pyspark_pipeline/config_utils.py)
        # os.path.dirname(__file__) is /app/pyspark_pipeline
        # os.path.join(..., '..') is /app
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        absolute_config_path = os.path.join(project_root, config_filename)

        logger.debug(f"Attempting to load configuration from: {absolute_config_path}")

        with open(absolute_config_path, 'r') as file_stream:
            config_data = yaml.safe_load(file_stream)

        if config_data is None: # Handle empty YAML file case
            logger.warning(f"Configuration file '{absolute_config_path}' is empty.")
            return {} # Return empty dict for empty YAML, consistent with no config found

        logger.info(f"Configuration loaded successfully from {absolute_config_path}")
        return config_data

    except FileNotFoundError:
        logger.error(f"Configuration file not found at {absolute_config_path}.")
        # Re-raise if needed by caller, or return None/empty dict
        # For this design, returning None to signal critical failure.
        return None
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration file {absolute_config_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading configuration from {absolute_config_path}: {e}")
        return None

if __name__ == '__main__':
    import sys
    # Adjust sys.path for direct execution to find pyspark_pipeline.logging_utils
    _project_root_for_test = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if _project_root_for_test not in sys.path:
        sys.path.insert(0, _project_root_for_test)

    from pyspark_pipeline.logging_utils import setup_logging
    setup_logging() # Default level is INFO

    logger.info("--- Testing config_utils.py ---")

    # Test loading the default config.yaml
    logger.info("Attempting to load default 'config.yaml':")
    config = load_config()
    if config:
        logger.info("Config loaded successfully. Sample values:")
        logger.info(f"  Application Name: {config.get('application', {}).get('name')}")
        logger.info(f"  Logging Level: {config.get('logging', {}).get('level')}")
        logger.info(f"  Input File Path: {config.get('input_data', {}).get('file_path')}")
        allowed_cats = config.get('processing_rules', {}).get('transformations', {}).get('allowed_categories', [])
        logger.info(f"  First Allowed Category: {allowed_cats[0] if allowed_cats else 'N/A'}")
    else:
        logger.error("Failed to load default 'config.yaml'.")

    logger.info("\n--- Testing with a non-existent config file ---")
    # Test loading a non-existent config file
    non_existent_config = load_config("non_existent_config.yaml")
    if non_existent_config is None:
        logger.info("Correctly handled non-existent config file: returned None.")
    else:
        logger.error("Incorrectly handled non-existent config file: did not return None.")

    logger.info("\n--- Testing with an invalid YAML file (simulated) ---")
    # To truly test yaml.YAMLError, we'd need to create a malformed file.
    # For now, we'll just note that the handler exists.
    # We can simulate by trying to load a non-YAML file as if it were YAML.
    # Example: try loading this python script itself as YAML (will cause YAMLError)
    logger.info("Attempting to load a non-YAML file ('config_utils.py') to trigger YAMLError:")
    invalid_yaml_config = load_config(__file__) # Pass its own path
    if invalid_yaml_config is None:
        logger.info("Correctly handled invalid YAML file: returned None (expected YAMLError).")
    else:
        logger.error("Incorrectly handled invalid YAML file: did not return None.")

    logger.info("\n--- config_utils.py tests finished ---")
