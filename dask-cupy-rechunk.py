import cudf
from distributed.utils import format_bytes
import dask.array as da
import numpy as np
import asyncio
import dask.dataframe as dd
from distributed import Scheduler, Worker, Client, Nanny
from distributed.utils import log_errors
import cupy

#async with Nanny(s.address, protocol='ucx', nthreads=1,
# nanny is really a worker running on a defined CUDA DEVICE
protocol = 'ucx'
async def f():
    async with Scheduler(protocol=protocol, interface='ib0',
    dashboard_address=':8789') as s:
        async with Nanny(s.address, protocol=protocol, nthreads=1,
                memory_limit='32GB',
                env={'CUDA_VISIBLE_DEVICES': '2'},
                ) as w:
            async with Nanny(s.address, protocol=protocol,memory_limit='32gb',
                    env={'CUDA_VISIBLE_DEVICES': '3'},
                    nthreads=1) as w2:
                async with Client(s.address, asynchronous=True) as c:
                    with log_errors(pdb=True):
                        # Create a simple random array
                        N = 500000000

                        chunks = N // 100
                        rs = da.random.RandomState(RandomState=cupy.random.RandomState)
                        x = rs.random(N, chunks=chunks)
                        y = rs.random(N, chunks=chunks)
                        def f(x):
                            return cudf.from_dlpack(x.toDlpack())
                        res_x = x.map_blocks(f, meta=cudf.Series())
                        res_y = y.map_blocks(f, meta=cudf.Series())
                        res = (res_x + res_y).persist()
                        await res
                        x.rechunk(chunks = N // 10).persist()
                        res = await c.compute(x.sum())
                        print(res)
                        print("FINISHED"*100)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(f())
