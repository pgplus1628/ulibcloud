from UserDict import UserDict
import json
import md5

class uMeta(UserDict):
    """
    store metadata of the erasure coded file blocks :
    name
    size : size in bytes
    blocksize
    k
    m
    stripe location : on which cloud it is saved
    md5
    hash : original file hash(md5)
    """

    def __init__(self):
        UserDict.__init__(self)
        #self["name"] = filename

    def save_to_string(self):
        s = json.dumps(self.data)
        return s
    
    def save_to_file(self, filename = None):
        if filename == None:
            filename = self["name"] + ".meta"
        s = self.save_to_string()
        open(filename, "w").write(s)
    
    def load_from_string(self, s):
        try :
            self.data = json.loads(s)    
        except ValueError :
            print(s)
    
    def load_from_file(self, filename):
        self.load_from_string(open(filename, "r").read())

    def set_name(self, name):
        self["name"] = name            

    def get_name(self):
        return self["name"]

    def set_size(self, size):
        self["size"] = size

    def get_size(self):
        return self["size"]

    def set_blocksize(self, blocksize):
        self["blocksize"] = blocksize
    
    def get_blocksize(self):
        return self["blocksize"]

    def set_k(self, k):
        self["k"] = k

    def set_m(self, m):
        self["m"] = m

    def set_stripe_location(self, sx, cloud_provider):
        #sx must be between 0 and m
        self['s' + str(sx)] = cloud_provider

    def get_stripe_location(self, sx) :
        return self['s' + str(sx)]

    def get_location_stripe(self, cloud_privider):
        ret = []
        for x in self.data.keys():
            entry = self.data[x] 
            if entry == cloud_privider :
                ret.append(x[1:])
        return ret

    def set_md5(self,cx,digest):
        #mx must be between 0 and m
        self['c' + str(cx)] = digest
    
    def get_md5(self, cx) :
        return self['c' + str(cx)]

    def set_cmeta(self, cmeta) :
        self['cmeta'] = cmeta

    def get_cmeta(self) :
        return self['cmeta']

    def set_hash(self, digest):
        self["hash"] = digest
    
    def get_hash(self) :
        return self["hash"]

    def del_item(self,key):
        del self[key]

    def cal_md5(self):
        keys = self.data.keys()
        keys.sort()
        s = ""
        for key in keys :
            s += str(key)
            s += str(self[key])
        return md5.new(s).hexdigest()
    
    def check_md5(self) :
        meta_md5_1 = self["cmeta"]
        tmp = self.get_cmeta()
        self.del_item("cmeta")
        meta_md5_2 = self.cal_md5()
        ret = meta_md5_1 == meta_md5_2
        self.set_cmeta(tmp)
        return ret

#for class test
if __name__ == "__main__":
    meta = uMeta()
    meta.set_name("test_file")
    meta.set_size(18880)
    meta.set_blocksize(1024)
    meta.set_k(3)
    meta.set_m(5)
    meta.set_hash('asdfasdfasdfasdf')
    meta.set_stripe_location("s0", "S3")
    meta.set_stripe_location("s1", "CloudFiles")
    meta.set_stripe_location("s2", "Google Cloud Storage")
    meta.set_stripe_location("s3", "S3")
    meta.set_stripe_location("s4", "S3")
    print meta.save_to_string()
    meta.save_to_file()    
    

    #Now load meta from the saved file.
    ameta = uMeta()
    ameta.load_from_file("test_file.meta")
    print meta.save_to_string()
    print ameta["name"]
    print ameta['hash']


