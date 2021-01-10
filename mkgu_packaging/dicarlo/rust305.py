from pathlib import Path

import numpy as np
import xarray as xr
import pandas as pd

import brainio_collection
from brainio_base.assemblies import NeuronRecordingAssembly
from brainio_collection.packaging import package_data_assembly


def main():
    single_nc_path = Path("/Users/jjpr/dev/dldata/scripts/rust_single.nc")
    da_single = xr.open_dataarray(single_nc_path)

    stimuli = brainio_collection.get_stimulus_set('dicarlo.Rust2012')

    da_single.name = 'dicarlo.Rust2012'

    print('Packaging assembly')
    package_data_assembly(da_single, assembly_identifier=da_single.name, stimulus_set_identifier=stimuli.identifier,
                          bucket_name='brainio.dicarlo')


if __name__ == '__main__':
    main()


