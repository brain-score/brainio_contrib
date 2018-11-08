import os
import tarfile
import zipfile

import numpy as np
import wget
from brainscore.knownfile import KnownFile as kf
from brainscore.lookup import pwdb
from brainscore.stimuli import ImageModel, AttributeModel, ImageMetaModel, StimulusSetModel, ImageStoreModel, \
    StimulusSetImageMap, ImageStoreMap
from tqdm import tqdm


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


def download_data(out_dir, remote_url, local_untarred_dir=None):
    filepath = os.path.join(out_dir, os.path.basename(remote_url))
    if not os.path.isfile(filepath):
        wget.download(remote_url, out=filepath)
    if local_untarred_dir and not os.path.isdir(os.path.join(out_dir, local_untarred_dir)):
        untar(filepath, out_dir=out_dir)


def untar(filepath, out_dir='.'):
    tar = tarfile.open(filepath, "r:gz")
    tar.extractall(path=out_dir)


def main():
    from mkgu_packaging.cifar.cifar10 import main as main10
    from mkgu_packaging.cifar.cifar100 import main as main100
    main10()
    main100()


if __name__ == '__main__':
    main()
