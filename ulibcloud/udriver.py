
from libcloud.storage.base import Object, Container
from libcloud.storage.types import ContainerIsNotEmptyError
from libcloud.storage.types import InvalidContainerNameError
from libcloud.storage.types import ContainerDoesNotExistError
from libcloud.storage.types import ObjectDoesNotExistError
from libcloud.storage.types import ObjectHashMismatchError
from libcloud.common.types import LibcloudError

from ulibcloud.types import *
from ulibcloud.ucontainer import uContainer
from ulibcloud.uobject import uObject
from ulibcloud.utils.files import encode_file_to_streams, write_streams_to_file, decode_files_to_file
from ulibcloud.utils.parallel import object_doer
from ulibcloud.umeta import uMeta

from os import path

import md5
import os

class uDriver :

    name = 'uDriver'
    hash_type = 'md5'
    block_size = 512 #16Bytes
    
    def __init__(self, driver_list, k, m):
        self.drivers = driver_list  
        self.k = k
        self.m = m
       
    def set_block_size(self, block_size) :
        self.block_size = block_size

    def list_container(self):
        """
        list container that contained by every provider of self.drivers.

        @rtype: C{list}
        @return : a list of C{uContainer}
        """ 
        dic_list = []
        ret_list = []
        for d in self.drivers :
            try :
                c_list = d.list_containers()
                tdic = {ele.name : ele for ele in c_list}
                dic_list.append(tdic)
            except LibcloudError, e :
                raise uLibcloudError(value=e.value, driver=e.driver)
            except Exception ,e :
                raise uLibcloudError(value='Unexpected Error: %s'
                                     % (e.message), driver=d)

        first_dic = dic_list[0]
        for name in first_dic.keys() :
            ok = True
            c_list = []
            for tdic in dic_list :
                if tdic.get(name) == None :
                    ok = False
                    break
                else :
                    c_list.append(tdic.get(name))
            if ok :
                ret_list.append(uContainer(name=name, container_list=c_list, 
                                           driver=self, extra=None))
        return ret_list


    def list_container_objects(self, container): 
        """
        @type container :C{uContainer}
        @param container : uContainer instance.
        
        @rtype : C{list}
        @return : list of uObject instances. In this function,  uObject.objects 
                  is meta list, not real object_list.
        """
        dic_list = []
        ret_list = []
        for c in container.containers :
            try :
                o_list = c.list_objects()
                tdic = {ele.name : ele for ele in o_list }
                dic_list.append(tdic)
            except LibcloudError, e :
                raise uLibcloudError(value=e.value, driver=e.driver)
            except Exception ,e :
                raise uLibcloudError(value='Unexpected Error: %s'
                                     % (e.message), driver=d)

        first_dic = dic_list[0]
        for name in first_dic.keys() :
            uobj_name = path.splitext(name)[0]
            if path.splitext(name)[1] != '.meta' :
                continue
            ok = True
            o_list = []
            for tdic in dic_list :
                if tdic.get(name) == None :
                    ok = False
                    break
                else :
                    o_list.append(tdic.get(name))
            if ok :
                ret_list.append(uObject(name = uobj_name, 
                                        object_list = o_list, # o_list is meta list 
                                        container = container,
                                        driver = self))
        return ret_list

    def get_container(self, container_name):
        """
        @type container_name : C{str}
        @param container_name : container's name.

        @rtype : C{uContainer}
        @return : C{uContainer} instance on success.
        """
        c_list = []
        for d in self.drivers :
            try :
                c_list.append(d.get_container(container_name))
                
            except ContainerDoesNotExistError, e :
                raise uContainerDoesNotExistError(value='container does not exist',
                                                  driver=e.driver,
                                                  container_name=e.container_name)
            except Exception, e :
                raise uLibcloudError(value='Unexpected Error: %s' % (e.message), 
                                     driver=d)

        return uContainer(name=container_name, container_list=c_list,
                          driver=self, extra=None)


    def get_object(self, container_name, object_name):
        """
        @type container_name : C{str}
        @param container_name : name of uContainer

        @type object_name : C{str}
        @param object_name : name of uObject.

        @rtype : C{uObject}
        @return : uObject. object_list is meta_list.
        """

        meta_name = object_name + '.meta'
        meta_list = []
        container_list = []
        for d in self.drivers :
            try :
                tobj = d.get_object(container_name, meta_name)
                tcon = d.get_container(container_name)
                meta_list.append(tobj)
                container_list.append(tcon)

            except LibcloudError ,e :
                raise uLibcloudError(value='Get object failed: %s' % (e.value), 
                                     driver = self)
        
            except Error , e :
                raise uLibcloudError(value='Unexpected Error: %s' % (e.message), 
                                     driver = self)
        return uObject(name = object_name, 
                      object_list = meta_list,
                      container = uContainer(container_name, container_list, self, extra = None),
                      driver = self)


    def get_meta(self, container_name, object_name, file_dir) :
        """
        @type file_dir : C{str}
        @param file_dir : dir of meta to download to. eg: /home/zork/ulib
               then metas will be donwloaded as /home/zork/ulib/object.meta.0
                                                /home/zork/ulib/object.meta.1,...
        @rtype : C{dict}
        @return : {'umeta' : C{uMeta}, 'priority' : C{list} } . get meta data of 
                  the giving object, priority is a list of drivers with download 
                  priority sorted by donwload speed.
        """
        # download meta
        meta_name = object_name + '.meta'
        meta_list = []
        meta_paths = []
        
        c_names = [ container_name for i in range(len(self.drivers)) ]
        m_names = [ meta_name for i in range(len(self.drivers)) ]

        ret_get = self._get_objects_parallel(self.drivers, c_names, m_names)
        if not ret_get['res'] :
            raise uLibcloudError(value='Get meta object failed : %s.' 
                                 % (ret_get['err'][0]) , driver=self)

        meta_list = ret_get['objects']
        for i in range(len(self.drivers)) :
            meta_paths.append(path.join(file_dir, meta_name + '.' + str(i)))

        ret = self._download_objects_parallel(meta_list, 
                                              meta_paths, 
                                              timing = True) 

        if not ret['res'] :
            raise uLibcloudError(value='Download meta failed : %s.' 
                                 % (ret['err'][0]) , driver=self)

        # check md5
        umeta = uMeta()
        meta_ok = False
        for i in range(self.m) :
            umeta.load_from_file(meta_paths[i])
            if umeta.check_md5() :
                meta_ok = True
                break

        if not meta_ok :
            raise uLibcloudError(value="Check meta md5 failed .", driver=self)

        # clean files
        self._clean_files(meta_paths)

        #get priority 
        p_list = [ (ret['cst'][i] , self.drivers[i])  for i in range(self.m)]
        p_list = sorted( p_list , key = lambda t : t[0] )
        return {'umeta' : umeta , 'priority' : [ x[1] for x in p_list] }


    def upload_object(self, file_path, container, object_name, extra=None, 
                      verify_hash=False):
        """
        Firstly, break the giving file to k files tripes, 
            and generate m-k redundant file stripes.
        Secondly, generate file meta info and dump it to file_name.meta 
        Finally, threading upload each file stripes to conresponding provider, 
            upload meta to every provider.
       
        @type file_path : C{str} 
        @param file_path : file abs path.

        @type container : C{uContainer}
        @param container : destination container to upload this file to.
        
        @type object_name : C{str}
        @param object_name : name of the  object to store on cloud.
        
        @type extra : C{dict}
        @param extra : extra info dict.
        
        @type verify_hash : C{bool}
        @param verify_hash : True , will verify file hash , the giving hash is 
            extra['data_hash'].

        @rtype : C{uObject}
        @return  : instance of uObject.
        """

        file = open(file_path, 'r')
        file_name = path.basename(file_path)
        streams = encode_file_to_streams(file_path, self.block_size, self.k, self.m)
        file_shares = write_streams_to_file(streams, file_path)
        
        meta = uMeta()
        meta.set_name(path.basename(file_name))
        meta.set_size(path.getsize(file_path))
        meta.set_blocksize(self.block_size)
        meta.set_k(self.k)
        meta.set_m(self.m)
        meta.set_hash(self._cal_md5(file.read()))
        for i in range(self.m) :
            meta.set_stripe_location(i, self.drivers[i].__class__.name)

        for i in range(self.m) :
            file_it = open(file_shares[i])
            meta.set_md5(i, self._cal_md5(file_it.read()))
        
        meta.set_cmeta(meta.cal_md5())
        meta.save_to_file(file_path + '.meta')

        #upload file shares
        threads = []
        for i in range(self.m) :
            callback = self.drivers[i].upload_object
            callback_args = {'file_path'   : file_shares[i] , 
                             'container'   : container.containers[i] , 
                             'object_name' : object_name + path.splitext(file_shares[i])[1] ,
                             'extra'       : extra ,
                             'verify_hash' : verify_hash }
            threads.append(object_doer(callback, callback_args, timing=False))

        for it in threads :
            it.start()
        for it in threads :
            it.join()

        rback_d = []
        rback_c = []
        rback_o = []
        ret_o = []
        ok = True
        error_list = []
        for i in range(len(threads)) : 
            it = threads[i]
            ret_o.append(it.ret)
            if it.emsg != None :
                ok = False
                error_list.append(it.emsg)
            else :
                rback_d.append(self.drivers[i])
                rback_c.append(container.name)
                rback_o.append(object_name)

        if not ok :
            self._roll_back_upload(rback_d, rback_c, rback_o)
            raise uLibcloudError(value='Upload object failed : %s.' % (error_list[0].value) , driver=self)
        
        #upload meta
        threads = []
        for i in range(self.m) :
            callback = self.drivers[i].upload_object
            callback_args = {'file_path'   : file_path + '.meta', 
                             'container'   : container.containers[i] , 
                             'object_name' : object_name + '.meta' ,
                             'extra'       : extra , 
                             'verify_hash' : verify_hash }
            threads.append(object_doer(callback, callback_args, timing=False))

        for it in threads :
            it.start()
        for it in threads :
            it.join()

        ok = True
        for i in range(len(threads)) : 
            it = threads[i]
            if it.emsg != None :
                ok = False
            else :
                rback_d.append(self.drivers[i])
                rback_c.append(container.name)
                rback_o.append(object_name)

        if not ok :
            self._roll_back_upload(rback_d, rback_c, rback_o)
            raise uLibcloudError(value='Upload object failed', driver=self)

        object_list = ret_o
#        object_list = []
#        for i in range(self.m) :
#            object_list.append(Object(name = path.basename(file_shares[i]), 
#                                      size = path.getsize(file_shares[i]) ,
#                                      hash = meta.get_md5(i) ,
#                                      container = container.containers[i], 
#                                      driver = self.drivers[i]))

        clean_list = []
        clean_list.append(file_path + '.meta')
        for i in range(self.m):
            clean_list.append(file_path + '.' + str(i))

        self._clean_files(clean_list)
        return uObject(file_name, object_list, container, self)
    

    def download_object(self, obj, destination_path, overwrite_existing=False,
                        delete_on_failure=True):
        """
        @type obj : C{uObject}
        @param obj : uObject instance.
        
        @type destination_path : C{str}
        @param destination_path : path to download file to.
        
        @type overwrite_existing : C{bool}
        @param overwrite_existing : True on overwrite existing files.
        
        @type delete_on_failure : C{bool}
        @param delete_on_failure : True on delete download files if failed.
        """
        
        #get meta
        dir_name = path.dirname(destination_path)
        
        ret_meta = self.get_meta(obj.container.name, obj.name, dir_name) 
        umeta = ret_meta['umeta']
        pri_list = ret_meta['priority']

        #download objects 
        objects = []
        suffixs = []
        cnt = 0
        for d in pri_list :
            if cnt > self.k :
                break
            tshare = umeta.get_location_stripe(d.__class__.name)
            d_s = []
            c_names = []
            o_names = []
            for i in range(len(tshare)) :
                if cnt >= self.k :
                    break
                suffixs.append(str(tshare[i]))
                d_s.append(d)
                c_names.append(obj.container.name)
                o_names.append(obj.name + '.' + str(tshare[i]))
                cnt = cnt + 1
        
            ret_get = self._get_objects_parallel(d_s, c_names, o_names)
            if not ret_get['res'] :
                raise uLibcloudError(value='Get objects failed : %s.' 
                                 % (ret_get['err'][0]) , driver=self)
            objects.extend(ret_get['objects'])

        base_name = path.basename(destination_path)
        if not base_name :
            destination_path = path.join(destination_path, obj.name)
            
        file_paths = [ destination_path + '.' + i for i in suffixs ]

        ret_down = self._download_objects_parallel(objects, file_paths , timing = False) 
        if not ret_down['res'] :
            raise uLibcloudError(value='Donwload object failed : %s.' 
                                 % (error_list[0].value) , driver=self)

        #decode files
        decode_files_to_file(file_paths, 
                             umeta.get_size(), 
                             umeta.get_blocksize(), 
                             self.k, 
                             self.m, 
                             destination_path)
        self._clean_files(file_paths)

    def delete_object(self, obj):
        """
        cannot rollback, so this operation is best effort delete, not atomic.
        @type obj : C{uObject}
        @param obj : uObject instance to delete.

        @rtype : C{dict}
        @return  : {'res' : C{bool}, 'err' : C{list} } 
                   True on delete success. False on partion deleted.
                   err is a list of C{uLibcloudError}, each represents an error.
        """

        container_name = obj.container.name
        object_name = obj.name
        err_list = []
        res = True
        # delete metas
        for d in self.drivers :
            _ret = self._delete_single_object(d, container_name, object_name + '.meta')
            if not _ret :
                res = False
                err_list.append(uLibcloudError(value = "Delete object %s failed on %s." 
                                               % (object_name + '.meta') , driver = d))

        # delete data shares
            # if lazy object , download meta, and  reinitialize obj
        if path.splitext(obj.object_list[0].name)[1] == '.meta' :
            object_list = []
            ret_meta = self.get_meta(obj.container.name, obj.name, path.abspath('.'))
            umeta = ret_meta['umeta']
            for d in self.drivers :
                shares = umeta.get_location_stripe(d.__class__.name)
                for s in shares :
                    sname = object_name + '.' + str(s)
                    _ret = self._delete_single_object(d, container_name, sname )
                    if not _ret :
                        res = False
                        err_list.append(uLibcloudError(value = "Delete object %s failed on %s." 
                                               % (sname) , driver = d))
        else :
            for o in obj.object_list :
                _ret = self._delete_single_object(o.driver, o.container.container_name, o.name)
                if not _ret :
                    res = False
                    err_list.append(uLibcloudError(value = "Delete object %s failed on %s." 
                                               % (o.name) , driver = o.dirver))

        return {'res' : res , 'err' : err_list }

    def create_container(self, container_name):
        """
        create container on provider list, if one is failed , all is falied.
        InvalidContainerNameError : container name already exist or contains invalid character.
        @type container_name : C{str}
        @param container_name : container's name.
        
        @rtype : C{uContainer}.
        @return : C{uContainer} instance on success.
        """
        containers = []
        for d in self.drivers :
            try:
                c = d.create_container(container_name)
                containers.append(c)
            except InvalidContainerNameError ,e :
                raise InvaliduContainerNameError(e.value, 
                                                 container_name=e.container_name,
                                                 driver=e.driver)
            except LibcloudError, e:
                raise uLibcloudError(e.value, e.driver)

            except Exception, e :    
                raise uLibcloudError('Unexpected error: %s' % (e.message), driver=d)
               
        return uContainer(name=container_name, container_list=containers, 
                          driver=self, extra=None)


    def delete_container(self, container):
        """
        All objects in the container must be deleted first.
        Delete container on multiple providers, 
        if any one is failed to delete, rollback.

        @type container: C{uContainer}
        @param container: uContainer to be deleted.

        @rtype: C{bool}
        @return : True on success.
        """
        successed = []
        ok = True
        for c in container.containers :
            try :
                if c.driver.delete_container(c) :
                    successed.append(c)
                else :
                    ok = False
            except ContainerIsNotEmptyError, e:
                ok = False
                # roll back
                for i in successed :
                    i.driver.create_container(container.name)
                raise uContainerIsNotEmpty(value=e.value, 
                                           container_name=e.container_name, 
                                           driver=e.driver)

        return ok


    def _cal_md5(self, content):
        return md5.new(content).hexdigest()

    def _delete_single_object(self, driver, container_name, object_name):
        try :
            obj = driver.get_object(container_name, object_name)
            ret = driver.delete_object(obj)
            return ret
        except Exception ,e :
            #TODO
            return False
            pass

    def _get_single_object(self, driver , container_name , object_name) :
        try :
            obj = driver.get_object(container_name, object_name )
            return obj
        except Exception ,e :
            #TODO
            pass

    def _get_objects_parallel(self, drivers , container_names, object_names, timing = False):
        threads = []
        for i in range(len(drivers)):
            callback = drivers[i].get_object
            callback_args = {'container_name' : container_names[i],
                             'object_name'   : object_names[i] }
            threads.append(object_doer(callback, callback_args, timing))

        for it in threads :
            it.start()
        for it in threads :
            it.join()

        ok = True
        error_list = []
        ret_list = []
        for it in threads : 
            if it.emsg != None :
                ok = False
            error_list.append(it.emsg)
            ret_list.append(it.ret)

        return {'res' : ok, 'objects' : ret_list, 'err' : error_list}


    def _roll_back_upload(self, driver, container_name, object_name):
        for i in range(len(driver)) :
            self._delete_single_object(driver[i], container_name[i], 
                                       object_name[i])

    def _roll_back_delete(self, driver, container_name, object_name ) :
        """
        it's hard to roll back, you shoud download deleted file first, 
        this cost is too much.
        """
        #TODO
        pass

    def _clean_files(self, file_paths) :
        """
        @type file_paths : C{list}
        @param file_paths : list of file_paths to clean.
        """
        for p in file_paths :
            try :
                os.remove(p)
            except Exception ,e :
                pass

    def _download_object(self, object, file_path):
        try :
            object.driver.download_object(destination_path = file_path, 
                                          overwrite_existing = True,
                                          delete_on_failure = True)
        except Exception ,e :
            return False
        return True
            
    def _download_objects_parallel(self, objects, file_paths , timing = False) :

        """
        @type objects : C{list}
        @param objects : a list of apache-liblcoud obeject.
        
        @type file_paths : C{list}
        @param file_paths : a list of destination_path to download to.

        @type timing : C{bool}
        @param timing : True on download timing.

        @rtype : C{dict}
        @return : dict{'res' : C{bool}, 'err' : C{list}, 'cst' : C{list} } 
                  res is True on download all success, err is a list of C{str} error
                  message, cst is a list of C{int} means download time.
        """
        threads = []
        for i in range(len(objects)):
            callback = objects[i].driver.download_object
            callback_args = {'obj' : objects[i],
                             'destination_path'   : file_paths[i] , 
                             'overwrite_existing'   : True,
                             'delete_on_failure' : True }
            threads.append(object_doer(callback, callback_args, timing))

        for it in threads :
            it.start()
        for it in threads :
            it.join()

        ok = True
        error_list = []
        cst_list = []
        for it in (threads) : 
            if it.emsg != None :
                ok = False
            error_list.append(it.emsg)
            cst_list.append(it.time)

        return {'res' : ok, 'err' : error_list, 'cst' : cst_list}

