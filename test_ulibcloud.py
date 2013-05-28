
from ulibcloud.udriver import uDriver as Driver
from ulibcloud.uprovider import get_driver
from ulibcloud.ext.ext_provider import extProvider
from ulibcloud.types import *

from libcloud.storage.types import Provider 

S3_eu_west = get_driver(Provider.S3_EU_WEST)
driver_s3_eu_west = S3_eu_west('your_key', 'your_secret')

Cloud_files_us = get_driver(Provider.CLOUDFILES_US)
driver_cloud_files_us = Cloud_files_us('your_key', 'your_secret')

k = 1 
m = 2 

driver = Driver([driver_s3_eu_west, driver_cloud_files_us], k, m)

def test_container_create(c_name):
    try :
        print 'create container %s' % (c_name)
        container = driver.create_container(c_name)
        return container
    except uLibcloudError ,e :
        print e

def test_container_get(c_name):
    try :
        print 'get container %s' % (c_name)
        container = driver.get_container(c_name)
        return container
    except uLibcloudError ,e :
        print e

def test_container_delete(container):
    try :
        print 'delete container %s' % (container.name)
        return driver.delete_container(container)
    except  uLibcloudError ,e :
        print e

def test_container_list():
    try :
        print 'list container '
        return driver.list_container()
    except uLibcloudError ,e :
        print e

def test_upload_object(file_path, container, object_name, extra):
    driver.upload_object(file_path, container, object_name, extra)

def test_list_container_objects(container):
    return driver.list_container_objects(container)

##################
def test_get_object(container_name, object_name):
    return driver.get_object(container_name, object_name)

def test_get_meta(container_name, object_name, file_dir):
    return driver.get_meta(container_name, object_name, file_dir)

def test_download_object(obj, destination_path):
    return driver.download_object(obj, destination_path)

def test_delete_object(obj):
    return driver.delete_object(obj)



def main() :

    c_name = 'iwanttofuckyou'
    f_path = '/home/zork/Dev-place/ulibcloud-0.1/lamport'
    d_path = '/home/zork/Desktop/'
    o_name = 'lamportx'
    extra = {'content_type' : 'ulib'}
#    container = test_container_create(c_name)
#    print container


    print '=== get container ===' 
    container = test_container_get(c_name)
    print container


#    ret = test_container_delete(container)
#    print ret 
    
#    container = test_container_create(c_name)
#    print container

#    c_list = test_container_list()
#    print c_list

#    print '=== list container ==='
#    container = test_container_get(c_name)
#    print container

#    print '=== upload object ==='
#    test_upload_object(f_path, container, o_name, extra)

#    print '=== list container object ==='
#    objects_list = test_list_container_objects(container)
#    for i in objects_list :
#        print i.name
    
    print '=== get object === '
    obj = test_get_object(c_name, o_name)
#    for i in obj.objects :
#        print i.name

    print '=== get meta ==='
    umeta = test_get_meta(c_name, o_name, d_path)
    print umeta


    print '=== download object ==='
    test_download_object(obj, d_path)

if __name__ == '__main__' :
    main()


