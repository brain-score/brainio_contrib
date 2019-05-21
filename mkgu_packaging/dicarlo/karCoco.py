import os

import h5py
import numpy as np
import pandas as pd
import scipy.misc
import xarray as xr
from result_caching import store
from sklearn.model_selection import StratifiedShuffleSplit
from tqdm import tqdm

from brainio_base.assemblies import BehavioralAssembly
from brainio_base.stimuli import StimulusSet
from mkgu_packaging.dicarlo.rajalingham2018objectome import create_image_zip, \
    add_stimulus_set_metadata_and_lookup_to_db, write_netcdf, add_assembly_lookup, upload_to_s3


@store(identifier_ignore=['stimuli_dir'])
def collect_stimuli(data_path, stimuli_dir):
    stimuli = []
    with h5py.File(data_path, 'r') as f:
        meta = f['meta']
        os.makedirs(stimuli_dir, exist_ok=True)
        for image_id, filename, reference, obj, sample_number, object_number, phase in tqdm(zip(
                *[meta[key][0] for key in ['id', 'filename', 'img', 'obj', 'sampleNr', 'objNr', 'phase']]),
                desc='stimuli', total=len(meta['img'][0])):
            img = np.array(f[reference])
            img = img.transpose([1, 2, 0])
            image_id = ''.join([chr(i) for i in f[image_id]])
            object_name = ''.join(chr(i) for i in f[f[obj][0][0]])
            sample_number = int(f[sample_number][0][0])
            object_number = int(f[object_number][0][0])
            filename = ''.join([chr(i) for i in f[filename]])
            phase = ''.join([chr(i) for i in f[phase]])  # only used for monkey training
            target_path = os.path.join(stimuli_dir, filename)
            scipy.misc.imsave(target_path, np.rot90(np.fliplr(img)))
            stimuli.append({
                'image_current_local_file_path': target_path,
                'image_path_within_store': filename,
                'image_id': image_id,
                'obj': object_name,
                'sample_number': sample_number,
                'object_number': object_number,
            })
    stimuli = StimulusSet(stimuli)
    return stimuli


@store(identifier_ignore=['stimuli'])
def collect_data(data_path, stimuli):
    responses = []
    with h5py.File(data_path, 'r') as f:
        data = f['data']
        keys = ['sample_number', 'sample_object_number', 'distractor_number', 'correct']
        for trial in tqdm(np.array(data['dataTurk'], dtype=int).transpose(), desc='trials'):
            response = {key: value for key, value in zip(keys, trial)}
            meta = stimuli[stimuli['sample_number'] == response['sample_number']]
            assert len(meta) == 1
            meta = meta.iloc[0]
            response['sample_obj'] = meta['obj']
            response['image_id'] = meta['image_id']
            distractor_meta = stimuli[stimuli['object_number'] == response['distractor_number']]
            distractor_obj = set(distractor_meta['obj'])
            assert len(distractor_obj) == 1
            response['distractor_obj'] = list(distractor_obj)[0]
            response['correct'] = bool(response['correct'])
            response['choice'] = response['sample_obj'] if response['correct'] else response['distractor_obj']
            del response['correct']  # cannot be serialized to netcdf
            responses.append(response)
    responses = pd.DataFrame(responses)
    responses = to_xarray(responses)
    return responses


def to_xarray(responses):
    columns = responses.columns
    responses = xr.DataArray(responses['choice'],
                             coords={column: ('presentation', responses[column]) for column in columns},
                             dims=['presentation'])
    responses = responses.set_index(presentation=columns)
    responses = BehavioralAssembly(responses)
    return responses


def main():
    data_path = '/braintree/home/msch/share/coco-behavior/coco_data.mat'
    target_dir = os.path.join(os.path.dirname(__file__), 'KarCoco')
    target_stimuli_dir = os.path.join(target_dir, 'stimuli')
    stimuli = collect_stimuli(data_path, stimuli_dir=target_stimuli_dir)
    assembly = collect_data(data_path, stimuli)
    assert len(stimuli) == 1600
    assert len(assembly) == 179660
    assert len(set(assembly['image_id'].values)) == 1600
    assert set(assembly['image_id'].values) == set(stimuli['image_id'])
    assert len(set(assembly['choice'].values)) == len(set(assembly['sample_obj'].values)) \
           == len(set(assembly['distractor_obj'].values)) == 10

    assembly.name = 'dicarlo.KarCoco'
    stimuli.name = 'dicarlo.karcoco'

    # split into train/test
    print("Splitting into train/test")
    split = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=15)
    train_idx, test_idx = next(split.split(stimuli['image_id'].values, stimuli['obj'].values))
    train_stimuli, test_stimuli = stimuli.iloc[train_idx], stimuli.iloc[test_idx]
    assert len(train_stimuli) == 1280 and len(test_stimuli) == 320
    train_stimuli.name, test_stimuli.name = stimuli.name + '.public', stimuli.name + '.private'
    train_assembly = assembly[{'presentation': [image_id in train_stimuli['image_id'].values
                                                for image_id in assembly['image_id'].values]}]
    test_assembly = assembly[{'presentation': [image_id in test_stimuli['image_id'].values
                                               for image_id in assembly['image_id'].values]}]
    assert len(train_assembly) == 143492 and len(test_assembly) == 36168
    train_assembly.name, test_assembly.name = assembly.name + '.public', assembly.name + '.private'

    upload(train_assembly, train_stimuli, target_dir)
    upload(test_assembly, test_stimuli, target_dir)


def upload(assembly, stimuli, target_dir, bucket_name='brainio-dicarlo'):
    store_name = assembly.name.lower().replace('.', '_')
    assembly_store_name = 'assy_' + store_name
    image_store_name = 'image_' + store_name
    # zip, meta
    print("Zipping stimuli")
    zip_basename = image_store_name + '.zip'
    zip_path = os.path.join(target_dir, zip_basename)
    zip_sha1 = create_image_zip(stimuli, zip_path)
    print("Adding StimulusSet lookup to db")
    # stimulus_set_model = add_stimulus_set_metadata_and_lookup_to_db(
    #     stimuli, stimulus_set_name=stimuli.name, bucket_name=bucket_name,
    #     zip_file_name=zip_basename, image_store_unique_name=image_store_name, zip_sha1=zip_sha1)
    print("Writing netcdf")
    netcdf_basename = assembly_store_name + '.nc'
    netcdf_path = os.path.join(target_dir, netcdf_basename)
    write_netcdf(assembly, netcdf_path)
    print("Adding assembly lookup to db")
    # add_assembly_lookup(assembly.name, stim_set_model=stimulus_set_model, bucket_name=bucket_name,
    #                     target_netcdf_file=netcdf_path, assembly_store_unique_name=assembly_store_name)
    # upload
    print("uploading to S3")
    # upload_to_s3(zip_basename, bucket_name=bucket_name, target_s3_key=zip_basename)
    # upload_to_s3(netcdf_path, bucket_name=bucket_name, target_s3_key=netcdf_basename)


if __name__ == '__main__':
    main()
