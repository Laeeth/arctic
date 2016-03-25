module arctic.compression;
import std.experimental.logger;
import arctic.lz4;

static this()
{
    logger = logging.getLogger(__name__);
    enum enableParallel = !os.environ.get("DISABLE_PARALLEL");
}

// switch to parallel LZ4 compress (and potentially other parallel stuff), Default True
__gshared bool enableParallel=true;
enum LZ4_N_PARALLEL = 50  # No. of elements to use parellel compression in LZ4 mode


/**
    Set the global multithread compression mode

    Parameters
    ----------
        mode: `bool`
            True: Use parallel compression. False: Use sequential compression
*/
void setParallel(bool mode)
{
    enableParallel=mode;
    logger.info("Setting parallelisation mode to {}" ~ mode?"multithread":"singlethread");
}

/**
    Compress an array of strings

    By default LZ4 mode is standard in interactive mode,
    and high compresion in applications/scripts
*/
def compressArray(string[] input)
{
    if (!enableParallel)
        return input.map!(entry=>entry.compress).array;

    // Less than 50 chunks its quicker to compress sequentially
    if (input.length > LZ4_N_PARALLEL)
        return compressArrayForce(input);
    else
        return input.map!(entry=>entry.compress).array;
}


def _get_lib():
    if ENABLE_PARALLEL:
        return clz4
    return lz4


def compress(_str):
    """
    Compress a string

    By default LZ4 mode is standard in interactive mode,
    and high compresion in applications/scripts
    """
    return _get_lib().compress(_str)


def decompress(_str):
    """
    Decompress a string
    """
    return _get_lib().decompress(_str)


def decompress_array(str_list):
    """
    Decompress a list of strings
    """
    if ENABLE_PARALLEL:
        return clz4.decompressarr(str_list)
    return [lz4.decompress(chunk) for chunk in str_list]


# cython: profile=True

#
# LZ4 code was copied from: https://github.com/steeve/python-lz4/ r8ac9cf9df8fb8d51f40a3065fa538f8df1c8a62a 22/4/2015 [tt]
#

cdef extern from "lz4.h":
    cdef int LZ4_compress(char* source, char* dest, int inputSize) nogil
    cdef int LZ4_compressBound(int isize) nogil
    cdef int LZ4_decompress_safe(const char* source, char* dest, int compressedSize, int maxOutputSize) nogil

cdef extern from "lz4hc.h":
    cdef int LZ4_compressHC(char* source, char* dest, int inputSize) nogil

cimport cython
cimport cpython
cimport libc.stdio
cimport openmp

from libc.stdlib cimport malloc, free, realloc
from libc.stdint cimport uint8_t, uint32_t
from libc.stdio cimport printf
from cython.view cimport array as cvarray
from cython.parallel import prange
from cython.parallel import threadid
from cython.parallel cimport parallel

cdef void store_le32(char *c, uint32_t x) nogil:
    c[0] = x & 0xff
    c[1] = (x >> 8) & 0xff
    c[2] = (x >> 16) & 0xff
    c[3] = (x >> 24) & 0xff

cdef uint32_t load_le32(char *c) nogil:
    cdef uint8_t *d = <uint8_t *>c
    return d[0] | (d[1] << 8) | (d[2] << 16) | (d[3] << 24)


cdef int hdr_size = sizeof(uint32_t)

cdef char ** to_cstring_array(list_str):
    """ Convert a python string list to a **char 
        Note: Performs a malloc. You must free the array once created.
    """ 
    cdef char **ret = <char **>malloc(len(list_str) * sizeof(char *))
    for i in xrange(len(list_str)):
        ret[i] = list_str[i]
    return ret


auto _compress(string pString, extern(C) int function(char *, char *, int) LZ4Compress)
{
    // sizes
    int originalSize = pString.length;

    // buffers
    char *cString =  pString.ptr;
    char *result;     // destination buffer
    ubyte[] dResult;

    // calc. estimated compresed size
    int compressedSize = LZ4_compressBound(originalSize)
    // alloc memory
    result = cast(char*) GC.malloc(compressedSize + hdrSize)
    // store original size
    storeLE32(result, originalSize);

    // compress & update size
    compressedSize = LZ4Compress(cString, result + hdr_size, original_size);
    # cast back into a python sstring
    dResult = cast(ubyte[])(result[0..compressed_size + hdr_size]);

    GC.free(result)

    return dResult;
}

auto decompress(string pString)
{
    int compressedSize = pStringlength;
    uint originalSize = cast(uint)load_le32(cString);
    char* result = cast(char*) GC.malloc(originalSize);
    LZ4_decompress_safe(pString.ptr + hdr_size, result, compressedSize - hdrSize, originalSize);
    auto dResult=cast(ubyte[])result[0..originalSize);
    free(result)
    return dResult;
}


def compressarr(pStrList):
    return _compressarr(pStrList, LZ4_compress)

def compressarrHC(pStrList):
    return _compressarr(pStrList, LZ4_compressHC)


cdef _compressarr(pStrList, int (*Fnptr_LZ4_compress)(char *, char *, int) nogil):
    
    if len(pStrList) == 0:
        return []

    cdef char **cStrList = to_cstring_array(pStrList)
    cdef Py_ssize_t n = len(pStrList)

    # loop parameters
    cdef char *cString
    cdef int original_size
    cdef uint32_t compressed_size
    cdef char *result
    cdef Py_ssize_t i

    # output parameters
    cdef char **cResult = <char **>malloc(n * sizeof(char *))
    cdef int[:] lengths = cvarray(shape=(n,), itemsize=sizeof(int), format="i")
    cdef int[:] orilengths = cvarray(shape=(n,), itemsize=sizeof(int), format="i")
    cdef bytes pyResult

    # store original string lengths
    for i in range(n):
        orilengths[i] = len(pStrList[i])

    with nogil, parallel():
        for i in prange(n, schedule='static'):
            cString = cStrList[i]
            original_size = orilengths[i]
            # calc. estaimted compresed size
            compressed_size = LZ4_compressBound(original_size)
            # alloc memory
            result = <char*>malloc(compressed_size + hdr_size)
            # store original size
            store_le32(result, original_size)
            # compress & update size
            compressed_size = Fnptr_LZ4_compress(cString, result + hdr_size, original_size)
            # assign to result
            lengths[i] = compressed_size + hdr_size
            cResult[i] = result

    # cast back to python
    result_list = []
    for i in range(n):
        pyResult = cResult[i][:lengths[i]]
        free(cResult[i])
        result_list.append(pyResult)

    free(cResult)
    free(cStrList)

    return result_list;
}

auto decompressarr(pStrList)
{
    if (pStrList.length == 0)
        return []

    char **cStrList = to_cstring_array(pStrList)
    auto n=pStrList.length;

    // loop parameters
    char* cString;
    uint originalSize,compressedSize;
    char *result;

    // output parameters
    char **cResult = <char **>malloc(n * sizeof(char *))
    int[:] clengths = cvarray(shape=(n,), itemsize=sizeof(int), format="i")
    int[:] lengths = cvarray(shape=(n,), itemsize=sizeof(int), format="i")
    ubyte[] dResult

    foreach(i;0..n)
        clengths[i] = len(pStrList[i])

    foreach(ref i;taskPool.parallel(iota(n, schedule='static'))
    {
        auto compressedSize = clengths[i]
        uint originalSize = cast(uint)load_le32(cStrList[i].ptr);
        result = cast(char*)malloc(originalSize);
        LZ4_decompress_safe(cString + hdr_size, result, compressed_size - hdr_size, original_size);
        cResult[i] = result;
        lengths[i] = originalSize;
    }

    resultList = []
    foreach(i;0..n)
    {
        pyResult = cResult[i][:lengths[i]];
        free(cResult[i]);
        result_list.append(pyResult);
    }

    free(cResult);
    free(cStrList);

    return result_list;
}