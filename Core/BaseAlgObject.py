import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import toml
from pathlib import Path


class Config:
    config_file_name = 'SysConfig.toml'

    @classmethod
    def get_config(cls):
        config = None
        try:
            base_path = Path(__file__).parent.parent
            config_file_name = os.path.join(base_path, cls.config_file_name)
            config_file_name = os.path.abspath(config_file_name)
            config = toml.load(config_file_name)
        except FileNotFoundError as e:
            print(f"@@@ Missing system configuration file '{cls.config_file_name}'")
            config = None
        return config


class MyLogger:
    @staticmethod
    def configure_log(use_console=True, name="App", folder='log', logger_name=None,
                      level=logging.DEBUG,
                      message_format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'):
        # date_format = "%y-%m-%d %H:%M:%S"
        date_format = "%H:%M:%S"
        formatter = logging.Formatter(fmt=message_format, datefmt=date_format)
        os.makedirs(folder, exist_ok=True)
        log_file = os.path.join(folder, '%s.log' % name)
        handlers = []
        file_handler = RotatingFileHandler(log_file, mode='w', encoding='utf8', maxBytes=1000000, backupCount=3)
        # file_handler = logging.FileHandler(log_file, mode='w', encoding='utf8')
        handlers.append(file_handler)
        if use_console:
            console_handler = logging.StreamHandler()
            handlers.append(console_handler)
        logger = logging.getLogger() if logger_name is None else logging.getLogger(logger_name)
        logger.setLevel(level)  # TODO from config
        for h in handlers:
            h.setLevel(level)
            h.setFormatter(formatter)
        logger.handlers = handlers
        # logger.addHandler(h)
        return logger


class BaseAlgObject:
    config = Config.get_config()
    root_module = sys.modules['__main__']
    try:
        file_name = root_module.__getattribute__('__file__')
        app_name = os.path.basename(root_module.__file__).split('.')[0]
    except AttributeError as ex:
        app_name = root_module.sys.argv[0].split('.')[0]
    logger = MyLogger.configure_log(name=app_name, logger_name=app_name)

