
from libcloud.storage.base import Object, Container



class BaseExtDriver(object) :

    name = None
    hash_type = 'md5'
    
    def __init__(self, key, secret=None, secure=True, host=None, port=None):
        pass

    def list_conatiner(self):
        """
        @return : A list of libcloud Container instances.
        """
        raise NotImplementedError(
              ' list_container not implemented for this driver')

    def list_container_objects(self, container) :
        
        """ 
        @type container: C{Container}
        @param container : libcloud Container instance.

        @return : A list of libcloud Object instances.
        """
        raise NotImplementedError(
              ' list_container_objects not implemented for this driver')

    def get_container(self, container_name) :
        """
        @type container_name : C{str}
        @param container_name : container's name.
        
        @return : C{Container} instance. libcloud Container.
        """
        raise NotImplementedError(
              ' get_container not implemented for this driver')

    def get_object(self, container_name, object_name):
        """
        @type container_name : C{str}
        @param container_name : container's name.

        @type object_name : C{str}
        @param object_name : object's name.
        
        @return: C{Object} instance. libcloud Object.
        """
        raise NotImplementedError(
              ' get_object not implemented for this driver')

    def download_object(self, obj, destination_path, overwrite_existing=False, delete_on_failure=True):
        """
        @type obj: C{Object}
        @param obj: libcloud Object instance
    
        @type destination_path: C{str}
        @param destination_path: full path where the incoming file will be saved.
        
        @overwrite_existing: C{bool}
        @param overwrite_existing: True to overwrite an existing file.

        @type delete_on_failure: C{bool}
        @param delete_on_failure: True to delete a paritially download file.
        
        @rtype: C{bool}
        @return : True if the object has been successfully downloaded.

        """
        raise NotImplementedError(
              ' download_object not implemented for this driver')


    def upload_object(self, file_path, container, object_name, extra=None, verify_hash=True):

        """
        @type file_path : C{str}
        @param file_path : full path of the uploading object.
        
        @type extra: C{dict}
        @param extra : (optional) extra attributes.

        """

        raise NotImplementedError(
              ' upload_object not implemented for this driver')

    def delete_object(self, obj):
        """
        @type obj: C{Object}
        @param obj: Object instance.

        @return: C{bool} True on success.
        """
        raise NotImplementedError(
            'delete_object not implemented for this driver')

    def create_container(self, container_name):
        """
        @type container_name: C{str}
        @param container_name: Container name.

        @return: C{Container} instance on success.
        """
        raise NotImplementedError(
            'create_container not implemented for this driver')

    def delete_container(self, container):
        """
        @type container: C{Container}
        @param container: Container instance

        @rtype: C{bool}
        @return: True on success, False otherwise.
        """
        raise NotImplementedError(
            'delete_container not implemented for this driver')

