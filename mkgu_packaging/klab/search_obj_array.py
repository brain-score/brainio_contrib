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

    arr_loc = loadmat(os.path.join(data_path / 'gt', 'array.mat'))

    for i in range(1, 301):
        j = (np.argmax(arr_loc['MyData'][i-1]['arraycate'][0] == arr_loc['MyData'][i-1]['targetcate'][0]))
        target_path = os.path.join(data_path / 'stimuli', 'array_' + str(i) + '.jpg')
        filename = 'array_' + str(i) + '.jpg'
        image_id = 'stimuli_' + str(i)
        image_label = 'stimuli'
        sample_number = i

        stimuli.append({
            'image_current_local_file_path': target_path,
            'image_path_within_store': filename,
            'image_label': image_label,
            'image_id': image_id,
            'tar_obj_pos': j,
            'sample_number': sample_number,
        })

    for i in range(1, 301):
        j = (np.argmax(arr_loc['MyData'][i-1]['arraycate'][0] == arr_loc['MyData'][i-1]['targetcate'][0]))
        target_path = os.path.join(data_path / 'target', 'target_' + str(i) + '.jpg')
        filename = 'target_' + str(i) + '.jpg'
        image_id = 'target_' + str(i)
        image_label = 'target'
        sample_number = i

        stimuli.append({
            'image_current_local_file_path': target_path,
            'image_path_within_store': filename,
            'image_label': image_label,
            'image_id': image_id,
            'tar_obj_pos': j,
            'sample_number': sample_number,
        })

    for i in range(1,7):
        target_path = os.path.join(data_path / 'gt', 'mask' + str(i) + '.jpg')
        filename = 'mask' + str(i) + '.jpg'
        image_id = 'mask_' + str(i)
        image_label = 'mask'
        sample_number = i

        stimuli.append({
            'image_current_local_file_path': target_path,
            'image_path_within_store': filename,
            'image_label': image_label,
            'image_id': image_id,
            'tar_obj_pos': i - 1,
            'sample_number': sample_number,
        })

    stimuli = StimulusSet(stimuli)

    stimuli.image_paths = {row.image_id: row.image_current_local_file_path for row in stimuli.itertuples()}
    stimuli['image_file_name']= stimuli['image_path_within_store']

    return stimuli

def collect_data(data_path, sub_id):
    image_id = ['stimuli_' + str(i) for i in range(1, 301)]
    subjects = []
    for i in sub_id:
        subjects += [i]*len(image_id)

    S_data = np.load(os.path.join(data_path / 'human_data', 'human_all.npy'))
    I_data = np.load(os.path.join(data_path / 'human_data', 'I_human_all.npy'))
    data = np.zeros((300*len(sub_id), 8, 2), dtype=int)
    data[:,:7,:] = S_data
    data[:,7,:] = I_data

    assembly = BehavioralAssembly(data,
                               coords={'image_id': ('presentation', image_id*len(sub_id)),
                                       'subjects': ('presentation', subjects),
                                       'fixation': [*range(8)],
                                       'position': ['x', 'y']},
                               dims=['presentation', 'fixation', 'position'])
    return assembly

def main():
    data_dir = Path(__file__).parent / 'search_datasets'
    data_path = data_dir / 'array'

    # create stimuli
    stimuli = collect_stimuli(data_path)
    stimuli.name = 'klab.Zhang2018.search_obj_array'

    # create assembly for different subjects
    assembly = collect_data(data_path, [*range(1, 16)])
    assembly.name = 'klab.Zhang2018search_obj_array'

    # package
    print("\nPackaging Stimuli ----------")
    package_stimulus_set(stimuli, stimulus_set_name=stimuli.name)

    print("\nPackaging Assembly ----------")
    package_data_assembly(assembly, data_assembly_name=assembly.name, stimulus_set_name=stimuli.name)


if __name__ == '__main__':
    main()
