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

                        # working!!!
                        # n_rows = 5000000
                        # n_keys = 500000
                        # 1200000 B or 1.2 MB

                        #n_rows = 5000000
                        #n_keys = 2500000


                        # n_rows = 5000
                        # n_keys = 2500


                        chunks = n_rows // 1000
                        left = dd.concat([
                            da.random.random(n_rows, chunks=chunks).to_dask_dataframe(columns='x'),
                            da.random.randint(0, n_keys, size=n_rows,chunks=chunks).to_dask_dataframe(columns='id'),
                            ], axis=1).persist()


                        right = dd.concat([
                            da.random.random(n_rows, chunks=chunks).to_dask_dataframe(columns='y'),
                            da.random.randint(0, n_keys, size=n_rows,
                                chunks=chunks).to_dask_dataframe(columns='id'),], axis=1).persist()

                        print('Building CUDF DataFrames...')
                        gright = right.map_partitions(cudf.from_pandas)
                        gleft = left.map_partitions(cudf.from_pandas)

                        print(gleft.npartitions)
                        print(gright.npartitions)
                        print('Repartition Left 10...')
                        res = gleft.repartition(npartitions=10)
                        res = await res.persist()
                        out = await c.compute(res.head(compute=False))
                        print(out)
                        print("FINISHED!")
                        # await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(f())
