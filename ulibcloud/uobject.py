
from libcloud.storage.base import Object
from umeta import uMeta

class uObject(object) :
    """ 
    Represents an object in ulibcloud .
    """
    def __init__(self, name, object_list, container, driver) :

        """
        @type name: C{name}
        @param name: object name.
        
        @type object_list : C{list}
        @param object_list : list of apache-liblcoud object.
    
        @type container : C{uContainer}
        @param container : object container. 

        @type driver : C{uDriver}
        @param driver : object driver.

        """
        self.name = name
        self.objects = object_list
        self.container = container
        self.driver = driver

    def download(self, destination_path, overwrite_existing=False,
                 delete_on_failure=True):
        return self.driver.download_object(self, destination_path, 
                                           overwrite_existing,
                                           delete_on_failure)
    def delete(self):
        return self.driver.delete_object(self)
    
    def __repr__(self):
        return ('<uObject: name=%s, size=%s, hash=%s, provider=%s ...>' %
                (self.name, self.size, self.hash, self.driver))




