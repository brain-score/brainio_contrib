import os
import zipfile
from pathlib import Path

import boto3
from brainio_collection.knownfile import KnownFile as kf
from brainio_collection.lookup import pwdb
from brainio_collection.stimuli import StimulusSetModel, ImageStoreModel, AttributeModel, ImageModel, \
    StimulusSetImageMap, ImageStoreMap, ImageMetaModel


def create_image_zip(proto_stimulus_set, target_zip_path):
    os.makedirs(os.path.dirname(target_zip_path), exist_ok=True)
    with zipfile.ZipFile(target_zip_path, 'w') as target_zip:
        for image in proto_stimulus_set.itertuples():
            target_zip.write(image.image_current_local_file_path, arcname=image.image_path_within_store)
    zip_kf = kf(target_zip_path)
    return zip_kf.sha1


def upload_to_s3(source_file_path, bucket_name, target_s3_key):
    client = boto3.client('s3')
    client.upload_file(source_file_path, bucket_name, target_s3_key)


def extract_specific(proto_stimulus_set):
    general = ['image_current_local_file_path', 'image_id', 'image_path_within_store']
    stimulus_set_specific_attributes =[]
    for name in list(proto_stimulus_set):
        if name not in general:
            stimulus_set_specific_attributes.append(name)
    return stimulus_set_specific_attributes


def add_image_metadata_to_db(proto_stimulus_set, stim_set_model, image_store_model):
    pwdb.connect(reuse_if_open=True)
    stimulus_set_specific_attributes = extract_specific(proto_stimulus_set)
    eav_attributes = {}

    for name in stimulus_set_specific_attributes:
        target_type = type(proto_stimulus_set[name].values.item(0)).__name__
        eav_attribute, created = AttributeModel.get_or_create(name=name, type=target_type)
        eav_attributes[name] = eav_attribute

    for image in proto_stimulus_set.itertuples():
        pw_image, created = ImageModel.get_or_create(image_id=image.image_id)
        StimulusSetImageMap.get_or_create(stimulus_set=stim_set_model, image=pw_image)
        ImageStoreMap.get_or_create(image=pw_image, image_store=image_store_model, path=image.image_path_within_store)
        for name in eav_attributes:
            ImageMetaModel.get_or_create(image=pw_image, attribute=eav_attributes[name], value=str(getattr(image, name)))


def add_stimulus_set_lookup_to_db(stimulus_set_name, bucket_name, zip_file_name,
                                               image_store_unique_name, zip_sha1):
    pwdb.connect(reuse_if_open=True)
    stim_set_model, created = StimulusSetModel.get_or_create(name=stimulus_set_name)
    image_store_model, created = ImageStoreModel.get_or_create(location_type="S3", store_type="zip",
                                                         location=f"https://{bucket_name}.s3.amazonaws.com/{zip_file_name}",
                                                         unique_name=image_store_unique_name,
                                                         sha1=zip_sha1)
    return (stim_set_model, image_store_model)


def add_stimulus_set_metadata_and_lookup_to_db(proto_stimulus_set, stimulus_set_name, bucket_name, zip_file_name,
                                               image_store_unique_name, zip_sha1):
    stim_set_model, image_store_model = add_stimulus_set_lookup_to_db(stimulus_set_name, bucket_name, zip_file_name,
                                  image_store_unique_name, zip_sha1)
    add_image_metadata_to_db(proto_stimulus_set, stim_set_model, image_store_model)
    return stim_set_model


def package_stimulus_set(proto_stimulus_set, stimulus_set_name, bucket_name="brainio-dicarlo"):
    """

    :param proto_stimulus_set: A DataFrame containing one row for each image, and the columns ['image_current_local_file_path', 'image_id', 'image_path_within_store'] and columns for all stimulus-set-specific metadata
    :param stimulus_set_name: A dot-separated string starting with a lab identifier.
    :param bucket_name: 'brainio-dicarlo' for DiCarlo Lab stimulus sets, 'brainio-contrib' for
    external stimulus sets.
    :return:
    """
    # * Create a zip file of the images (respecting image_path_within_store) and upload it to S3
    # * Add StimulusSet lookup data to the lookup database
    # * Add per-image metadata to the lookup database
    image_store_unique_name = "image_" + stimulus_set_name.replace(".", "_")
    zip_file_name = image_store_unique_name + ".zip"
    target_zip_path = Path(__file__).parent / zip_file_name
    s3_key = zip_file_name

    sha1 = create_image_zip(proto_stimulus_set, str(target_zip_path))
    upload_to_s3(str(target_zip_path), bucket_name, s3_key)
    stimulus_set_model = add_stimulus_set_metadata_and_lookup_to_db(proto_stimulus_set, stimulus_set_name, bucket_name, zip_file_name, image_store_unique_name, sha1)

# functions to add:  (see mkgu_packaging/dicarlo/rajalingham2018objectome.py)
# to_xarray
# write_netcdf
# add_assembly_lookup
# upload_to_s3
# package_data_assembly


