
from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider 
from libcloud.storage.types import LibcloudError

S3_eu_west = get_driver(Provider.S3_EU_WEST)
driver_s3_eu_west = S3_eu_west('your_key', 'your_secret')

Cloud_files_us = get_driver(Provider.CLOUDFILES_US)
driver_cloud_files_us = Cloud_files_us('your_key', 'your_secret')




#driver = driver_s3_eu_west
driver = driver_cloud_files_us

def test_container_create(c_name):
    try :
        print 'create container %s' % (c_name)
        container = driver.create_container(c_name)
        return container
    except LibcloudError ,e :
        print e

def test_container_get(c_name):
    try :
        print 'get container %s' % (c_name)
        container = driver.get_container(c_name)
        return container
    except LibcloudError ,e :
        print e

def test_container_delete(container):
    try :
        print 'delete container %s' % (container.name)
        return driver.delete_container(container)
    except  LibcloudError ,e :
        print e

def test_container_list(container):
    try :
        print 'list container '
        return driver.list_container_objects(container)
    except LibcloudError ,e :
        print e

def main() :
    c_name = 'iwanttofuckyou'
    d_path = '/home/zork/Desktop/'
#    container = test_container_create(c_name)
#    print container

    container = test_container_get(c_name)
    print container


#    ret = test_container_delete(container)
#    print ret 
    
#    container = test_container_create(c_name)
#    print container

    c_list = test_container_list(container)
    print c_list

    for o in c_list :
        o.download(d_path)

#    container = test_container_get(c_name)
#    print container



if __name__ == '__main__' :
    main()









