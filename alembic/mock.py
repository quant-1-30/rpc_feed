
cdef int scale
cdef float zero_point

cdef int fakequant(float x):
     cdef int quant
     quant = <int>x/scale + zero_point
     return quant

cdef float dequant(int x):
    cdef float dequant
    dequant = <float>(x - zero_point) * scale
    return dequant


# cython
# -128 / 127
# 0 255
# layer / channel

