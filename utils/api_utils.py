import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('utils.api')

def retry_n_times(n, action_name, fallback):
    def perform(func):
        def wrapper(*args, **kwargs):
            for i in range(n + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"{action_name} FAILURE attempt_number={i + 1} ex={e}")
            logger.error(f"{action_name} RETRIES_EXPIRED")
            return fallback(*args, **kwargs)
        return wrapper
    return perform
