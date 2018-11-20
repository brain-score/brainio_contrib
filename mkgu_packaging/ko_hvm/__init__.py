import os

import h5py
import numpy as np
import xarray as xr
from brainscore.assemblies import NeuronRecordingAssembly


def load_responses(response_file, stimuli_ids):
    responses = h5py.File(response_file, 'r')
    assemblies = []
    neuroid_id_offset = 0
    for monkey in responses.keys():
        spike_rates = responses[monkey]['rates']
        assembly = xr.DataArray(spike_rates.value,
                                coords={
                                    'image_num': ('image_id', list(range(spike_rates.shape[0]))),
                                    'image_id': ('image_id', stimuli_ids),
                                    'neuroid_id': ('neuroid', list(
                                        range(neuroid_id_offset, neuroid_id_offset + spike_rates.shape[1]))),
                                    'region': ('neuroid', ['IT'] * spike_rates.shape[1]),
                                    'monkey': ('neuroid', [monkey] * spike_rates.shape[1]),
                                    'repetition': list(range(spike_rates.shape[2])),
                                },
                                dims=['image_id', 'neuroid', 'repetition'])
        assemblies.append(assembly)
        neuroid_id_offset += spike_rates.shape[1]
    assembly = xr.concat(assemblies, 'neuroid')
    assembly = assembly.stack(presentation=['image_id', 'repetition'])
    assembly = NeuronRecordingAssembly(assembly)
    assert len(assembly['presentation']) == 640 * 63
    assert len(np.unique(assembly['image_id'])) == 640
    assert len(assembly.sel(monkey='nano')['neuroid']) == len(assembly.sel(monkey='magneto')['neuroid']) == 288
    assert len(assembly['neuroid']) == len(np.unique(assembly['neuroid_id'])) == 288 * 2

    return assembly


def write_netcdf(assembly, target_netcdf_file):
    assembly.reset_index(assembly.indexes.keys(), inplace=True)
    assembly.to_netcdf(target_netcdf_file)


def main():
    ids_file = os.path.join(os.path.dirname(__file__), 'hvm640_ids.mat')
    stimuli_ids = h5py.File(ids_file, 'r')
    stimuli_ids = [''.join(chr(c) for c in stimuli_ids[stimuli_ids['hvm640_ids'].value[0, i]])
                   for i in range(stimuli_ids['hvm640_ids'].value[0].size)]

    neural_file = os.path.join(os.path.dirname(__file__), 'hvm640_neural.h5')
    assembly = load_responses(neural_file, stimuli_ids)

    target_netcdf_file = os.path.join(os.path.dirname(__file__), 'hvm640_ko.nc')
    write_netcdf(assembly, target_netcdf_file)
    print(f"wrote {target_netcdf_file}")

    return assembly


if __name__ == '__main__':
    main()
