import os, datetime, logging

class SQLServerHandler(logging.Handler):

    def __init__(self, db_connection, file_name, project_dir):
        super().__init__()
        self.db_connection = db_connection
        self.file_name = file_name
        self.project_dir = project_dir

    def emit(self, record):
        try:
            conn = self.db_connection
            cursor = conn.cursor()
            log_message = self.format(record)
            date_ = datetime.datetime.fromtimestamp(record.created).strftime('%Y-%m-%d')
            time_ = datetime.datetime.fromtimestamp(record.created).strftime('%H:%M:%S.%f')[:12]
            cursor.execute("INSERT INTO [nooredenadb].[extra].[logs] (date, time, directory, file_name, error_level, log_message, line) "
                           "VALUES (?, ?, ?, ?, ?, ?, ?)",
                           (date_, time_,
                            self.project_dir, self.file_name,
                            record.levelname, log_message, record.lineno))
            cursor.close()
        except Exception as e:
            print(f"Logging to database failed: {e}")


def get_logger(db_connection, file_name, project_dir, level = None, formatter: str = None, reset: bool = True,
               enable_console: bool = True, enable_file: bool = True, log_file_path: str = "bi_log.log") -> logging.Logger:

    if level is None:
        level = logging.ERROR

    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    if reset:
        logger.handlers.clear()

    if formatter is None:
        log_message_format = "%(asctime)s - %(levelname)s - %(filename)s - line: %(lineno)d - %(message)s"
    else:
        log_message_format = formatter
    log_formatter = logging.Formatter(log_message_format)

    ########## SQL Handler ##########
    sql_handler = SQLServerHandler(db_connection=db_connection, file_name=file_name, project_dir=project_dir)
    sql_handler.setFormatter(log_formatter)
    logger.addHandler(sql_handler)

    ########## File Handler ##########
    if enable_file:
        log_file_path = os.path.abspath(log_file_path)
        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)

    ########## Console Handler ##########
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        logger.addHandler(console_handler)

    return logger

if __name__ == '__main__':
    import sys, database
    logger = get_logger(db_connection=database.make_connection(), file_name="custom_logger.py",
                        project_dir=os.path.dirname(os.path.abspath(sys.argv[0])))
    logger.error("Testing Log for \"custom_logger.py\"")
