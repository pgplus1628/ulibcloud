
import sys
from zfec import Encoder
from zfec import Decoder
from os import path
from pyutil import mathutil



def encode_file_to_streams(file_path, block_size, k, m) :
    """
    break file into file stripes.
    @type file_path : C{str}
    @param file_path : abs path of file to encode.
    
    @type block_size : C{int}
    @param block_size : encode unit.

    @type k : C{int}
    @param k : number of  necessary file shares to restore.

    @type m : C{int}
    @param m : m-k redundant file shares.
    
    @rtype : C{list}
    @return : m file streams, each stream is a str.
    """
    file_size = path.getsize(file_path)
    file = open(file_path)
    fencoder = Encoder(k, m)
    block_count = mathutil.div_ceil(file_size, block_size)
    
    ds = []
    ds.extend([''] * k)

    # for the last round , block might be not complete or empty
    index = 0
    results = []
    results.extend([""] * m)

    for i in range(block_count / k):
        for index in range(k):
            ds[index] = file.read(block_size)
        ds[k-1] = ds[k-1] + "\x00" * (block_size - len(ds[k-1]))
        temp = fencoder.encode(ds)
        for j in range(m):
            results[j] = results[j] + temp[j]

    if block_count % k == 0 :
        return results

    #the last round
    for i in range(block_count % k):
        ds[i] = file.read(block_size)

    ds[i] = ds[i] + "\x00" * (block_size - len(ds[i]))
    for index in range(i+1,k):
        ds[index] = "\x00" * (len(ds[0]))

    temp = fencoder.encode(ds)
    for j in range(m):
        results[j] = results[j] + temp[j]

    return results

def write_streams_to_file(streams, file_path):
    """
    write each stream in streams to corresponding file.
    @type streams : C{list} 
    @param streams : list of str, each str is a stream.
    
    @type file_path : C{str}
    @param file_path : file abs path to write file to, each stream is write to
                       file_path + '.' +str(i).

    @rtype : C{list}
    @return : a list of file abs path has wrote streams to.
    """

    files = []
    files.extend([None] * len(streams))
    file_names = []
    file_names.extend([''] * len(streams))

    for i in range(len(streams)) :
        file_names[i] = '%s.%d' % (file_path, i)
        files[i] = open(file_names[i], 'w')
        files[i].write(streams[i])
        files[i].close
    return file_names


def decode_files_to_file(files, size, block_size, k, m, destination_path):
    """
    decoded file stripes to file.
    @type files :C{list}
    @param files : list of abs path of file stripes , 
                   should end with '.n' , n is an integer.

    @type size : C{int}
    @param size : file original size.

    @type block_size : C{int}
    @param block_size : processing unit.

    @type k : C{int}
    @param k : k.
    
    @type m : C{int}
    @param m : m.
    
    @type destination_path : C{str}
    @param destination_path : abs path to put decoded file to.
    """
    
    fdecoder = Decoder(k, m)
    file = open(destination_path, 'w')
    parts = [ int(path.splitext(f)[1][1:]) for f in files ]
    streams = []
    streams.extend([''] * k)
   
    file_shares = [ open(f,'r') for f in files ]
    file_shares_size = path.getsize(files[0]) * k
    block_count = mathutil.div_ceil(file_shares_size, block_size)

    for count in range(block_count):
        for i in range(k) :
           streams[i] = file_shares[i].read(block_size)
    
        results = fdecoder.decode(streams, parts)
        for i in range(len(results)) :
            file.write(results[i])

    file.truncate(size)
    file.close()


def decode_streams_to_file():
    """
    decode streaming data to file.
    """
    pass



def test():

    file_name = 'test_file'
    size = path.getsize(file_name)
    dst_name = file_name + '.restore'
    k = 3
    m = 5
    block_size = 512 #16Byte
    streams = encode_file_to_streams(file_name, block_size, k, m)
    file_names = write_streams_to_file(streams, file_name)
    print(file_names)

    decode_files_to_file(file_names[:k], size, block_size, k, m, dst_name)
    
    


if __name__ == '__main__':
    test()



