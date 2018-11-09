import os
import re
from glob import glob

import h5py
import numpy as np
import pandas as pd
import xarray as xr
from brainscore.assemblies import NeuronRecordingAssembly
from brainscore.knownfile import KnownFile as kf
from result_caching import store
from tqdm import tqdm


@store(identifier_ignore=['stimuli_directory'])
def collect_stimuli(stimuli_directory):
    meta = os.path.join(stimuli_directory, 'cocogray_labels.mat')
    meta = h5py.File(meta, 'r')
    labels = [''.join(chr(c) for c in meta[meta['lb'].value[0, i]]) for i in range(meta['lb'].value[0].size)]
    stimuli = []
    for image_file_path in tqdm(glob(os.path.join(stimuli_directory, '*.png'))):
        image_file_name = os.path.basename(image_file_path)
        image_number = re.match('im([0-9]+).png', image_file_name)
        image_number = int(image_number.group(1))
        im_kf = kf(image_file_path)
        stimuli.append({
            'image_id': im_kf.sha1,
            'image_file_name': image_file_name,
            'image_file_path': image_file_path,
            'image_file_sha1': im_kf.sha1,
            'image_number': image_number,
            'image_store_path': f"coco-gray/{image_file_name}",
            'label': labels[image_number],
        })
    stimuli = pd.DataFrame(stimuli)
    assert len(stimuli) == 1600
    assert len(np.unique(stimuli['image_id'])) == len(stimuli)
    return stimuli


def load_responses(response_file, stimuli):
    responses = h5py.File(response_file, 'r')
    assemblies = []
    for monkey in responses.keys():
        spike_rates = responses[monkey]['rates']
        assembly = xr.DataArray(spike_rates.value,
                                coords={
                                    'image_num': ('image_id', list(range(spike_rates.shape[0]))),
                                    'image_id': ('image_id', [
                                        stimuli['image_id'][stimuli['image_number'] == num].values[0]
                                        for num in range(spike_rates.shape[0])]),
                                    'neuroid_id': ('neuroid', list(range(spike_rates.shape[1]))),
                                    'region': ('neuroid', ['IT'] * spike_rates.shape[1]),
                                    'monkey': ('neuroid', [monkey] * spike_rates.shape[1]),
                                    'repetition': list(range(spike_rates.shape[2])),
                                },
                                dims=['image_id', 'neuroid', 'repetition'])
        assemblies.append(assembly)
    assembly = xr.concat(assemblies, 'neuroid')
    assembly = assembly.stack(presentation=['image_id', 'repetition'])
    assembly = NeuronRecordingAssembly(assembly)
    assert len(assembly['presentation']) == 1600 * 45
    assert len(np.unique(assembly['image_id'])) == 1600
    assert len(assembly.sel(monkey='nano')['neuroid']) == len(assembly.sel(monkey='magneto')['neuroid']) == 288
    assert len(assembly['neuroid']) == 288 * 2

    return assembly


def write_netcdf(assembly, target_netcdf_file):
    assembly.reset_index(assembly.indexes.keys(), inplace=True)
    assembly.to_netcdf(target_netcdf_file)


def main():
    stimuli_dir = os.path.join(os.path.dirname(__file__), 'stimuli')
    stimuli = collect_stimuli(stimuli_dir)

    neural_file = os.path.join(os.path.dirname(__file__), 'cocoGray_neural.h5')
    assembly = load_responses(neural_file, stimuli)

    target_netcdf_file = os.path.join(os.path.dirname(__file__), 'coco.nc')
    write_netcdf(assembly, target_netcdf_file)
    print(f"wrote {target_netcdf_file}")

    return stimuli, assembly


if __name__ == '__main__':
    main()
