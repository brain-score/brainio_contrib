import os

import h5py
import numpy as np
import pandas as pd
import scipy.misc
import xarray as xr
from pathlib import Path
from result_caching import store
from sklearn.model_selection import StratifiedShuffleSplit
from tqdm import tqdm
from scipy.io import loadmat

from brainio_base.assemblies import BehavioralAssembly
from brainio_base.stimuli import StimulusSet
from brainio_contrib.packaging import package_stimulus_set, package_data_assembly


def collect_stimuli(data_path):
    stimuli = []

    arr_loc = loadmat(os.path.join(data_path, 'gt/array.mat'))

    for i in range(1, 301):
        j = (np.argmax(arr_loc['MyData'][i-1]['arraycate'][0] == arr_loc['MyData'][i-1]['targetcate'][0]))
        target_path = os.path.join(data_path, 'stimuli/array_' + str(i) + '.jpg')
        filename = 'array_' + str(i) + '.jpg'
        image_label = 'stimuli'
        sample_number = i

        stimuli.append({
            'image_current_local_file_path': target_path,
            'image_path_within_store': filename,
            'image_label': image_label,
            'tar_obj_pos': j,
            'sample_number': sample_number,
        })

    for i in range(1, 301):
        j = (np.argmax(arr_loc['MyData'][i-1]['arraycate'][0] == arr_loc['MyData'][i-1]['targetcate'][0]))
        target_path = os.path.join(data_path, 'target/target_' + str(i) + '.jpg')
        filename = 'target_' + str(i) + '.jpg'
        image_label = 'target'
        sample_number = i

        stimuli.append({
            'image_current_local_file_path': target_path,
            'image_path_within_store': filename,
            'image_label': image_label,
            'tar_obj_pos': j,
            'sample_number': sample_number,
        })

    for i in range(1,7):
        target_path = os.path.join(data_path, 'gt/mask' + str(i) + '.jpg')
        filename = 'mask' + str(j) + '.jpg'
        image_label = 'gt'
        sample_number = i

        stimuli.append({
            'image_current_local_file_path': target_path,
            'image_path_within_store': filename,
            'image_label': image_label,
            'tar_obj_pos': i - 1,
            'sample_number': sample_number,
        })

    stimuli = StimulusSet(stimuli)
    return stimuli

def collect_data(data_path, sub_id):
    arr_loc = loadmat(os.path.join(data_path, 'human_data/sub_' + str(sub_id) + '.mat'))

    data = np.zeros((300, 7, 2), dtype=int)
    for i in range(300):
        j = arr_loc['x'][0][i].reshape((-1,)).shape[0]
        data[i, :j, 0] = arr_loc['x'][0][i].reshape((-1,))
        data[i, :j, 1] = arr_loc['y'][0][i].reshape((-1,))

    assembly = BehavioralAssembly(data,
                                coords={'image_id': [*range(1, 301)],
                                        'fixation': [*range(7)],
                                        'position': ['x', 'y']},
                                dims=['image_id', 'fixation', 'position'])
    return assembly


def main():
    data_dir = Path(__file__).parent / 'search_datasets'
    data_path = data_dir / 'array'

    # create stimuli
    stimuli = collect_stimuli(data_path)
    stimuli.name = 'klab.Zhang2018.search_obj_array'

    # create assembly for different subjects
    assemblies = []
    for i in range(1, 16):
        assembly = collect_data(data_path, sub_id = i)
        assembly.name = 'klab.Zhang2018search_obj_array_sub_' + str(i)
        assemblies.append(assembly)

    # package
    print("Packaging")
    package_stimulus_set(stimuli, stimulus_set_name=stimuli.name)
    for assembly in assemblies:
        package_data_assembly(assembly, data_assembly_name=assembly.name, stimulus_set_name=stimuli.name)


if __name__ == '__main__':
    main()
