import logging
import os


def get_logger(name):

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(f"{log_dir}/pipeline.log"),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(name)