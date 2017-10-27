"""exceptions.py: navitron-cron custom exceptions"""

class NavitronCronException(Exception):
    """base exception for navitron-cron project"""
    pass

class ConnectionException(NavitronCronException):
    """base exception for connections issues"""
    pass
class FatalCLIExit(NavitronCronException):
    """general exception for fatal issue"""
    pass
class MissingMongoConnectionInfo(ConnectionException):
    """Unable to connect to mongo, missing connection info"""
    pass
class NoSDEDataFound(ConnectionException):
    """Blank collection found where SDE was expected"""
    pass
