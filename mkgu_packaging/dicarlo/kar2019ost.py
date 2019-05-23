import os

import h5py
import numpy as np
import pandas as pd
import scipy.misc
import scipy.misc
import scipy.misc
import xarray as xr
from result_caching import store
from tqdm import tqdm

from brainio_base.assemblies import DataAssembly
from brainio_base.stimuli import StimulusSet
from brainio_collection.knownfile import KnownFile
from mkgu_packaging.dicarlo.rajalingham2018objectome import add_stimulus_set_metadata_and_lookup_to_db, \
    add_assembly_lookup, upload_to_s3
from mkgu_packaging.dicarlo.rajalingham2018objectome import create_image_zip, \
    write_netcdf

object_lookup = {
    1: 'bear',
    2: 'elephant',
    3: 'person',
    4: 'car',
    5: 'dog',
    6: 'apple',
    7: 'chair',
    8: 'plane',
    9: 'bird',
    10: 'zebra',
}


@store(identifier_ignore=['stimuli_dir'])
def collect_stimuli(data_path, stimuli_dir):
    stimuli = []
    with h5py.File(data_path, 'r') as f:
        images, objects = f['images'], f['obj']
        os.makedirs(stimuli_dir, exist_ok=True)
        for image_num, (image, obj) in tqdm(enumerate(zip(images, objects[0])), desc='stimuli', total=len(images)):
            image = image.transpose([1, 2, 0])
            obj = object_lookup[obj]
            filename = f"image_{image_num:04d}.jpg"
            target_path = os.path.join(stimuli_dir, filename)
            scipy.misc.imsave(target_path, np.rot90(np.fliplr(image)))
            im_kf = KnownFile(target_path)
            stimuli.append({
                'image_current_local_file_path': target_path,
                'image_path_within_store': filename,
                "image_file_sha1": im_kf.sha1,
                "image_id": im_kf.sha1,
                'image_num': image_num,
                'image_label': obj,
            })
    stimuli = StimulusSet(stimuli)
    return stimuli


@store(identifier_ignore=['stimuli'])
def collect_data(data_path, stimuli):
    data = []
    with h5py.File(data_path, 'r') as f:
        osts, i1s = f['ost'], f['i1']
        for ost, i1, stimuli_row in tqdm(zip(osts[0], i1s[0], stimuli.itertuples()), total=len(osts), desc='trials'):
            row = {'ost': ost, 'i1': i1, 'image_id': stimuli_row.image_id, 'image_label': stimuli_row.image_label}
            data.append(row)
    data = pd.DataFrame(data)
    data = to_xarray(data)
    return data


def to_xarray(data):
    columns = data.columns
    data = xr.DataArray(data['ost'],
                        coords={column: ('presentation', data[column]) for column in columns},
                        dims=['presentation'])
    data = data.set_index(presentation=columns)
    data = DataAssembly(data)
    return data


def main():
    data_path = os.path.join(os.path.dirname(__file__), 'Kar2019OST', 'dataset.h5')
    target_dir = os.path.join(os.path.dirname(__file__), 'Kar2019OST')
    target_stimuli_dir = os.path.join(target_dir, 'stimuli')
    stimuli = collect_stimuli(data_path, target_stimuli_dir)
    assembly = collect_data(data_path, stimuli)

    assert len(stimuli) == len(assembly) == 1320
    assert len(set(assembly['image_id'].values)) == 1318
    assert set(assembly['image_id'].values) == set(stimuli['image_id'])
    assert len(set(assembly['image_label'].values)) == 10

    assembly.name = 'dicarlo.Kar2019'
    stimuli.name = 'dicarlo.Kar2019'
    upload(assembly, stimuli, target_dir)


def upload(assembly, stimuli, target_dir, bucket_name='brainio-dicarlo'):
    store_name = assembly.name.lower().replace('.', '_').replace(':', '_').replace(' ', '_')
    assembly_store_name = 'assy_' + store_name
    image_store_name = 'image_' + store_name
    # zip, meta
    stimuli.to_pickle(os.path.join(target_dir, image_store_name) + '.pkl')
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
