
cdef class BtFeed:

    cdef object _providers
    cdef object _pattern

    cdef object pipeline

    cpdef void load(self, str graph_xml, str dataset_path, str prefix, bint parallel=?) except *
