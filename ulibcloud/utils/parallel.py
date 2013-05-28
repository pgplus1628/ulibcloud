import time
import threading
from ulibcloud.types import uLibcloudError
from libcloud.common.types import LibcloudError
from libcloud.storage.types import ObjectDoesNotExistError

class p_doer(threading.Thread):
    """
    multiple thread do.
    """
    
    def __init__(self, callback, callback_kwargs) : 
        """
        @type callback : C{Function}
        @param callback : function which is called with the passed callback_kwargs.
        
        @type callback_kwargs : C{dict}
        @param callback_kwargs : Keyword argmuments which are passed to callback.

        """
        threading.Thread.__init__(self)
        self.callback = callback
        self.callback_kwargs = callback_kwargs
        self.emsg = None

    def run(self):
        try :
            self.ret = self.callback(**self.callback_kwargs)
        except Exception ,e :
            self.emsg = uLibCloudError(e.message, driver=None)
#            raise uLibcloudError(e.message, driver=None)

class object_doer(threading.Thread):
    """
    do object operations with multiple thread.
    """
    
    def __init__(self, callback, callback_kwargs, timing=False) : 
        """
        ret, emsg, time.
        @type callback : C{Function}
        @param callback : function which is called with the passed callback_kwargs.
        
        @type callback_kwargs : C{dict}
        @param callback_kwargs : Keyword argmuments which are passed to callback.

        @type timing : C{bool}
        @param timing : True on counting time to self.time.
        """
        threading.Thread.__init__(self)
        self.callback = callback
        self.callback_kwargs = callback_kwargs
        self.timing = timing
        self.emsg = None
        self.ret = None
        self.time = float(9999999999)

    def run(self):

        if self.timing :
            t0 = time.time()
            t1 = float(9999999999)
        try :
            self.ret = self.callback(**self.callback_kwargs)
        except LibcloudError ,e:
            self.emsg = uLibcloudError(e.value, driver=e.driver)
        except Exception ,e :
            self.emsg = uLibcloudError(e.message, driver=self.callback_kwargs.get('driver'))
        if self.timing :
            t1 = time.time()
            self.time = t1 - t0
#            print '%s : %s' % (self.name, str(self.time) )

class container_doer(threading.Thread):
    pass

