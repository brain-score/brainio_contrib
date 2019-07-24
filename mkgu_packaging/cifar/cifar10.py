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


def collect_stimuli_batch(pickle_file, stimuli_type, label_names, batch=1, write_image=True):
    with open(pickle_file, 'rb') as f:
        data = pickle.load(f, encoding='bytes')
    images = np.asarray(data[b'data'].T).astype('uint8')
    labels = data[b'labels']
    label_names = [label_names[label_index] for label_index in labels]
    stimuli = []
    stimuli_dir = os.path.join(os.path.dirname(pickle_file), stimuli_type)
    os.makedirs(stimuli_dir, exist_ok=True)
    for i in tqdm(range(images.shape[1]), desc=f'{stimuli_type}: {batch}'):
        image_file_name = f'{stimuli_type}-{(batch - 1) * 10000 + i}.png'
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
            'batch': batch,
            'image_file_sha1': im_kf.sha1,
            'image_current_local_file_path': image_file_path,
            'label': labels[i],
            'label_name': label_names[i],
        })
    stimuli = pd.DataFrame(stimuli)
    assert len(stimuli) == 10000
    assert len(np.unique(stimuli["image_id"])) == len(stimuli)
    return stimuli


@store(identifier_ignore=['write_image'])
def collect_stimuli(stimuli_directory, write_image=True):
    batches_path = os.path.join(stimuli_directory, 'cifar-10-batches-py')
    with open(os.path.join(batches_path, 'batches.meta'), 'rb') as f:
        label_names = pickle.load(f)['label_names']
    test = collect_stimuli_batch(os.path.join(batches_path, 'test_batch'), stimuli_type='test',
                                 label_names=label_names, write_image=write_image)
    train = None
    for batch in [1, 2, 3, 4, 5]:
        train_batch = collect_stimuli_batch(os.path.join(batches_path, f'data_batch_{batch}'), stimuli_type='train',
                                            batch=batch, label_names=label_names,
                                            write_image=write_image)
        train = pd.concat([train, train_batch]) if train is not None else train_batch
    stimuli = pd.concat([train, test])
    assert len(stimuli) == 60000
    assert len(stimuli[stimuli['type'] == 'test']) == 10000
    assert len(stimuli[stimuli['type'] == 'train']) == 50000
    return stimuli


def main():
    stimuli_dir = os.path.dirname(__file__)
    download_data(out_dir=stimuli_dir, remote_url='https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz',
                  local_untarred_dir='cifar-10-batches-py')
    stimuli = collect_stimuli(stimuli_dir, write_image=True)
    stimuli = StimulusSet(stimuli)
    stimuli.name = "cifar10"

    print("packaging")
    package_stimulus_set(stimuli, stimulus_set_name=stimuli.name, bucket_name="brainio-contrib")


if __name__ == '__main__':
    main()
