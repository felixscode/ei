from logging import getLogger,DEBUG,INFO,WARNING,ERROR,CRITICAL,StreamHandler



def match_level(level):
    match level:
        case 'DEBUG'|"debug"|0:
            return DEBUG
        case 'INFO'|'info'|1:
            return INFO
        case 'WARNING'|'warning'|2:
            return WARNING
        case 'ERROR'|'error'|3:
            return ERROR
        case 'CRITICAL'|'critical'|4:
            return CRITICAL
        case _:
            return INFO

def get_logger(name,level='INFO',handler=StreamHandler()):
    logger = getLogger(name)
    level = match_level(level)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger