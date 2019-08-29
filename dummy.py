from numba import cuda
from distributed.protocol.cuda import cuda_serialize, cuda_deserialize
import numpy as np
import cupy

class DummyColumn(object):

    def __init__(self):
        import threading
        self.lock = threading.Lock()
        pass

    def serialize(self):
        header = {}
        frames = [cuda.to_device(np.arange(100000))]
        # frames = [cupy.asarray(np.arange(100000))] # <- failing at size 100000
        return header, frames

    @classmethod
    def deserialize(cls, header, frames):
        return np.array([1, 2, 3])

@cuda_serialize.register(DummyColumn)
def serialize(col):
    print("I AM A DUMMY")
    return col.serialize()

@cuda_deserialize.register(DummyColumn)
def deserialize(header, frames):
    (frame,) = frames
    # TODO: put this in ucx... as a kind of "fixup"
#     breakpoint()
#     try:
#         frame.typestr = '<i8'
#         frame.shape = (3,)
#     except AttributeError:
#         pass
#     arr = cupy.asarray(frame)
#     print(arr, type(arr))
    return DummyColumn.deserialize(header, frames)