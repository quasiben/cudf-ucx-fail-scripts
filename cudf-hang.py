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
                        n_rows = 50000000
                        n_keys = 5000000


                        chunks = n_rows // 10
                        x = da.random.random(n_rows, chunks=chunks).to_dask_dataframe(columns='x').persist()
                        y = da.random.random(n_rows, chunks=chunks).to_dask_dataframe(columns='x').persist()

                        print('Building CUDF DataFrames...')
                        # gright = right.map_partitions(cudf.from_pandas)
                        # gleft = left.map_partitions(cudf.from_pandas)
                        def f(x):
                            # print(len(x))
                            try:
                                return cudf.from_pandas(x)
                            except:
                                print("FAILURE!!!")
                                print(x)

                        res_x = x.map_partitions(f).persist(workers=[w.worker_address])
                        res_y = y.map_partitions(f).persist(workers=[w2.worker_address])
                        # res = await gleft.persist()
                        await res_x
                        await res_y
                        res = await (res_x + res_y).persist()
                        print("FINISHED")
                        # out = await c.compute(res.sum())
                        # print(out)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(f())
