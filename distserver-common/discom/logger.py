import logging

def setup_logger(log_level=logging.DEBUG):
    """
    Configures and returns a logger that writes to the console.

    This function should be called once when the worker initializes.
    """
    # Define the format for log messages. We include a placeholder for 'request_id'
    # which will be added contextually for each job.
    log_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - [Request: %(request_id)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Get the root logger
    logger = logging.getLogger("runpod_worker")
    logger.setLevel(log_level)
    
    # --- Console Handler ---
    # This handler sends logs to standard output, which Runpod captures as worker logs.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    
    # Add the console handler to the logger
    # Check if handlers are already added to avoid duplication on hot reloads
    if not logger.handlers:
        logger.addHandler(console_handler)
    
    return logger