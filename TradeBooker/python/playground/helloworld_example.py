import utils.env_utils as envu
logger = envu.get_logger()

def main():
    logger.info("Hello World!")

if __name__ == "__main__":
    logger.info("main script being run...")
    main()