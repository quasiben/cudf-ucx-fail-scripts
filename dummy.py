from numba import cuda
from distributed.protocol.cuda import cuda_serialize, cuda_deserialize
import numpy as np
import cupy

from librmm_cffi import librmm as rmm

class DummyColumn(object):

    def __init__(self, size, kind):
        self.size = size
        self.kind = kind

    def serialize(self):
        header = {}
        if self.kind == "numba":
            frames = [cuda.to_device(np.arange(self.size))]
        elif self.kind == "rmm":
            frames = [rmm.to_device(np.arange(self.size))]
        elif self.kind == "cupy":
            frames = [cupy.asarray(np.arange(self.size))]
        else:
            frames = [np.arange(self.size)]
        return header, frames

    @classmethod
    def deserialize(cls, header, frames):
        return None

@cuda_serialize.register(DummyColumn)
def serialize(col):
    return col.serialize()

@cuda_deserialize.register(DummyColumn)
def deserialize(header, frames):
    (frame,) = frames
    return DummyColumn.deserialize(header, frames)