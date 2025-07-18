import logging

def setup_logger(
        name: str
) -> logging.Logger:
    
    logger = logging.getLogger(name)

    if not logger.handlers:  
        logger.setLevel(logging.INFO)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(module)s - %(message)s")
        stream_handler.setFormatter(formatter)

        logger.addHandler(stream_handler)
        logger.propagate = False 

    return logger   