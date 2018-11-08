import os
import pickle
import tarfile
import zipfile

import numpy as np
import pandas as pd
import wget
from PIL import Image
from brainscore.knownfile import KnownFile as kf
from brainscore.lookup import pwdb
from brainscore.stimuli import ImageModel, AttributeModel, ImageMetaModel, StimulusSetModel, ImageStoreModel, \
    StimulusSetImageMap, ImageStoreMap
from tqdm import tqdm


def collect_stimuli_batch(pickle_file, stimuli_type, label_names, batch=1, write_image=True):
    with open(pickle_file, 'rb') as f:
        data = pickle.load(f, encoding='bytes')
    images = np.asarray(data[b'data'].T).astype('uint8')
    labels = data[b'labels']
    label_names = [label_names[label_index] for label_index in labels]
    stimuli = []
    stimuli_dir = os.path.join(os.path.dirname(pickle_file), stimuli_type)
    os.makedirs(stimuli_dir, exist_ok=True)
    for i in tqdm(range(10000), desc=f'{stimuli_type}: {batch}'):
        image_file_name = f'{(batch - 1) * 10000 + i}.png'
        image_file_path = os.path.join(stimuli_dir, image_file_name)
        if write_image:
            image = Image.fromarray(images[:, i].reshape(3, 32, 32).transpose([1, 2, 0]))
            image.save(image_file_path)
        im_kf = kf(image_file_path)
        stimuli.append({
            'image_id': im_kf.sha1,
            'image_file_name': image_file_name,
            'type': stimuli_type,
            'batch': batch,
            'image_file_path': image_file_path,
            'image_file_sha1': im_kf.sha1,
            'image_store_path': f"cifar10_stimuli/{stimuli_type}/{image_file_name}",
            'label': labels[i],
            'label_name': label_names[i],
        })
    stimuli = pd.DataFrame(stimuli)
    assert len(stimuli) == 10000
    assert len(np.unique(stimuli["image_id"])) == len(stimuli)
    return stimuli


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


def create_image_zip(stimuli, target_zip_path):
    os.makedirs(os.path.dirname(target_zip_path), exist_ok=True)
    with zipfile.ZipFile(target_zip_path, 'w') as target_zip:
        for image in tqdm(stimuli.itertuples(), desc='zip images', total=len(stimuli)):
            target_zip.write(image.image_file_path, arcname=image.image_store_path)
    zip_kf = kf(target_zip_path)
    return zip_kf.sha1


def add_image_lookup(stimuli, target_zip_path, zip_sha1, stimulus_set_name, image_store_unique_name, bucket_name):
    pwdb.connect(reuse_if_open=True)
    zip_file_name = os.path.basename(target_zip_path)

    image_store, created = ImageStoreModel.get_or_create(location_type="S3", store_type="zip",
                                                         location=f"https://{bucket_name}.s3.amazonaws.com/{zip_file_name}",
                                                         unique_name=image_store_unique_name,
                                                         sha1=zip_sha1)
    stim_set_model, created = StimulusSetModel.get_or_create(name=stimulus_set_name)
    columns_attribute = {}
    for column in [col for col in stimuli.columns if col not in {'image_file_path', 'image_store_path'}]:
        column_type = stimuli[column].dtype
        attribute_type = {np.int64: "int", np.object_: "str"}[column_type.type]
        columns_attribute[column] = AttributeModel.get_or_create(name=column, type=attribute_type)

    for image in tqdm(stimuli.itertuples(), desc='upload'):
        pw_image, created = ImageModel.get_or_create(image_id=image.image_id)
        StimulusSetImageMap.get_or_create(stimulus_set=stim_set_model, image=pw_image)
        ImageStoreMap.get_or_create(image=pw_image, image_store=image_store, path=image.image_store_path)
        for column, attribute in columns_attribute.items():
            ImageMetaModel.get_or_create(image=pw_image, attribute=attribute, value=str(image[column]))

    return stim_set_model


def download_data(out_dir, remote_url='https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz'):
    filepath = os.path.join(out_dir, os.path.basename(remote_url))
    if not os.path.isfile(filepath):
        wget.download(remote_url, out=filepath)
    if not os.path.isdir(os.path.join(out_dir, 'cifar-10-batches-py')):
        untar(filepath, out_dir=out_dir)


def untar(filepath, out_dir='.'):
    tar = tarfile.open(filepath, "r:gz")
    tar.extractall(path=out_dir)


def main():
    stimuli_dir = os.path.dirname(__file__)
    download_data(out_dir=stimuli_dir)
    stimuli = collect_stimuli(stimuli_dir, write_image=False)

    stimulus_set_name = "cifar10"
    bucket_name = "brain-score-cifar"
    image_store_unique_name = "image_cifar10_stimuli"
    target_zip_path = os.path.join(os.path.dirname(__file__), image_store_unique_name + ".zip")

    zip_sha1 = create_image_zip(stimuli, target_zip_path)
    add_image_lookup(stimuli, target_zip_path, zip_sha1, stimulus_set_name, image_store_unique_name, bucket_name)

    return stimuli


if __name__ == '__main__':
    main()
