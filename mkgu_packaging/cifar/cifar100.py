import os
import pickle

import numpy as np
import pandas as pd
from PIL import Image
from result_caching import store
from tqdm import tqdm

from brainio_base.stimuli import StimulusSet
from brainio_collection.knownfile import KnownFile as kf
from brainio_contrib.packaging import package_stimulus_set
from mkgu_packaging.cifar import download_data


def collect_stimuli_batch(pickle_file, stimuli_type, coarse_label_names, fine_label_names, write_image=True):
    with open(pickle_file, 'rb') as f:
        data = pickle.load(f, encoding='bytes')
    images = np.asarray(data[b'data'].T).astype('uint8')
    coarse_labels, fine_labels = data[b'coarse_labels'], data[b'fine_labels']
    coarse_label_names = [coarse_label_names[label_index] for label_index in coarse_labels]
    fine_label_names = [fine_label_names[label_index] for label_index in fine_labels]
    stimuli = []
    stimuli_dir = os.path.join(os.path.dirname(pickle_file), stimuli_type + '-images')
    os.makedirs(stimuli_dir, exist_ok=True)
    for i in tqdm(range(images.shape[1]), desc=stimuli_type):
        image_file_name = f'{stimuli_type}-{i}.png'
        image_file_path = os.path.join(stimuli_dir, image_file_name)
        if write_image:
            image = Image.fromarray(images[:, i].reshape(3, 32, 32).transpose([1, 2, 0]))
            image.save(image_file_path)
        im_kf = kf(image_file_path)
        stimuli.append({
            'image_id': im_kf.sha1,
            'image_file_name': image_file_name,
            'image_path_within_store': image_file_name,
            'type': stimuli_type,
            'image_file_sha1': im_kf.sha1,
            'image_current_local_file_path': image_file_path,
            'coarse_label': coarse_labels[i],
            'fine_label': fine_labels[i],
            'coarse_label_name': coarse_label_names[i],
            'fine_label_name': fine_label_names[i],
        })
    stimuli = pd.DataFrame(stimuli)
    assert len(stimuli) == (10000 if stimuli_type == 'test' else 50000)
    # no test for len(np.unique(image_id)) == len(stimuli). there are duplicates.
    return stimuli


@store(identifier_ignore=['write_image'])
def collect_stimuli(stimuli_directory, write_image=True):
    data_path = os.path.join(stimuli_directory, 'cifar-100-python')
    with open(os.path.join(data_path, 'meta'), 'rb') as f:
        meta = pickle.load(f)
        coarse_label_names = meta['coarse_label_names']
        fine_label_names = meta['fine_label_names']
    test = collect_stimuli_batch(os.path.join(data_path, 'test'), stimuli_type='test',
                                 coarse_label_names=coarse_label_names, fine_label_names=fine_label_names,
                                 write_image=write_image)
    train = collect_stimuli_batch(os.path.join(data_path, 'train'), stimuli_type='train',
                                  coarse_label_names=coarse_label_names, fine_label_names=fine_label_names,
                                  write_image=write_image)
    stimuli = pd.concat([train, test])
    assert len(stimuli) == 60000
    assert len(stimuli[stimuli['type'] == 'test']) == 10000
    assert len(stimuli[stimuli['type'] == 'train']) == 50000
    return stimuli


def main():
    stimuli_dir = os.path.dirname(__file__)
    download_data(out_dir=stimuli_dir, remote_url='https://www.cs.toronto.edu/~kriz/cifar-100-python.tar.gz',
                  local_untarred_dir='cifar-100-python')
    stimuli = collect_stimuli(stimuli_dir, write_image=True)
    stimuli = StimulusSet(stimuli)
    stimuli.name = "cifar100"

    print("packaging")
    package_stimulus_set(stimuli, stimulus_set_name=stimuli.name, bucket_name="brainio-contrib")


if __name__ == '__main__':
    main()
