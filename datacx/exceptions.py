class ParamsMissingException(Exception):
    def __init__(self,message):
        self.message = message

class ConfigMissingException(Exception):
    def __init__(self,message):
        self.message = message

class ExtensionNotSupportException(Exception):
    def __init__(self,message):
        self.message = message

class UnSupportedDataSourceException(Exception):
    def __init__(self,message):
        self.message = message