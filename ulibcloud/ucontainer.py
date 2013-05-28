
class uContainer(object) :
    """
    Represent a container (bucket) which can hold multiple objects.
    """

    def __init__(self, name, container_list, driver, extra) :
        """
        @type name : C{str}
        @param name : container name
    
        @type container_list: C{list}
        @param container_list: list of libcloud containers.
    
        @type driver : C{uDriver}
        @param driver : driver of this container
    
        @type extra : C{dict}
        @param extra : extra attributes
        """ 
        self.name = name
        self.containers = container_list
        self.driver = driver
        self.extra = extra or {}
    
    def list_objects(self) :
        return self.driver.list_container_objects(container = self)

    def get_object(self, object_name):
        return self.driver.get_object(container_name = self.name, 
                                      object_name = object_name)

    def upload_object(self, file_path, object_name, extra = None) :
        return self.driver.upload_object(file_path, self, object_name, extra)

    def download_object(self, obj, destination_path, overwrite_existing=False,
                        delete_on_failure=True):
        return self.driver.download_object(obj, destination_path)
        #TODO 

    def delete_object(self, obj):
        return self.driver.delete_object(obj)

    def delete(self):
        return self.driver.delete_container(self)

    def __repr__(self):
        return ('<uContainer: name=%s provider=%s>'
                 % ( self.name, self.driver))



