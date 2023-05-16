import logging

logger = logging.getLogger("email_processor")
logger.setLevel(logging.INFO)

consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s – %(name)s – [%(levelname)s]: %(message)s", datefmt="%d-%m-%Y %H:%M:%S"
)

consoleHandler.setFormatter(formatter)
logger.addHandler(consoleHandler)
