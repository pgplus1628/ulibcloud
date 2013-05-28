
from libcloud.common.types import LibcloudError

__all__ = ['uLibcloudError', 
           'uContainerError',
           'uContainerAlreadyExistsError',
           'uContainerDoesNotExistError',
           'uContainerIsNotEmptyError', 
           'uObjectDoesNotExistError',
           'InvaliduContainerNameError']

class uLibcloudError(Exception):
    """ 
    The base class for ulibcloud exceptions 
    """
	
    def __init__(self, value ,driver = None):
        self.value = value
        self.driver = driver
    def __str__(self):
        return ("<uLibcloudError in"
                + repr(self.driver)
                + " "
                + repr(self.value) + ">")

class uContainerError(uLibcloudError) :
    error_type = 'uContainerError'
    
    def __init__(self, value, driver, container_name) :
        self.container_name = container_name
        super(uContainerError, self).__init__(value = value, driver = driver)

    def __str__(self):
        return ('<%s in %s, ucontainer=%s, value=%s>' %
                (self.error_type, repr(self.driver.name),
                 self.container_name, self.value))

class uObjectError(uLibcloudError):
    error_type = 'uObjectError'
    
    def __init__(self, value, driver, object_name):
        self.object_name = object_name
        super(uObjectError, self).__init__(value = value, driver = driver)

    def __str__(self):
        return ('<%s in %s, value=%s, object=%s>' %
                (self.error_type, repr(self.driver.name),
                 self.value, self.object_name))


class uContainerAlreadyExistsError(uContainerError):
    error_type = 'uContainerAlreadyExistsError'

class uContainerDoesNotExistError(uContainerError):
    error_type = 'uContainerDoesNotExistError'

class uContainerIsNotEmptyError(uContainerError):
    error_type = 'uContainerIsNotEmptyError'

class uObjectDoesNotExistError(uObjectError):
    error_type = 'uObjectDoesNotExistError'

class uObjectHashMismatchError(uObjectError):
    error_type = 'uObjectHashMismatchError'

class InvaliduContainerNameError(uContainerError):
    error_type = 'InvaliduContainerNameError'

