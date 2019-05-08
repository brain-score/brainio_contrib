import os
import zipfile
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


def extract_specific(proto_stimulus_set):
    general = ['image_current_local_file_path', 'image_id', 'image_path_within_store']
    stimulus_set_specific_attributes =[]
    for name in list(proto_stimulus_set):
        if name not in general:
            stimulus_set_specific_attributes.extend(name)
    return stimulus_set_specific_attributes


def add_image_metadata_to_db(proto_stimulus_set, stim_set_model, image_store_model):
    pwdb.connect(reuse_if_open=True)
    stimulus_set_specific_attributes = extract_specific(proto_stimulus_set)
    eav_attributes = {}

    for name in stimulus_set_specific_attributes:
        eav_attribute, created = AttributeModel.get_or_create(name=name,
                                                              type=proto_stimulus_set[name].dtype.name)
        eav_attributes[name] = eav_attribute

    for image in proto_stimulus_set.itertuples():
        pw_image, created = ImageModel.get_or_create(image_id=image.image_id)
        StimulusSetImageMap.get_or_create(stimulus_set=stim_set_model, image=pw_image)
        ImageStoreMap.get_or_create(image=pw_image, image_store=image_store_model, path=image.image_path_within_store)
        for name in eav_attributes:
            ImageMetaModel.get_or_create(image=pw_image, attribute=eav_attributes[name], value=str(getattr(image, name)))


def add_stimulus_set_metadata_and_lookup_to_db(proto_stimulus_set, stimulus_set_name, bucket_name, zip_file_name,
                                               image_store_unique_name, zip_sha1):
    pwdb.connect(reuse_if_open=True)
    stim_set_model, created = StimulusSetModel.get_or_create(name=stimulus_set_name)
    image_store_model, created = ImageStoreModel.get_or_create(location_type="S3", store_type="zip",
                                                         location=f"https://{bucket_name}.s3.amazonaws.com/{zip_file_name}",
                                                         unique_name=image_store_unique_name,
                                                         sha1=zip_sha1)
    add_image_metadata_to_db(proto_stimulus_set, stim_set_model, image_store_model)
    return stim_set_model


def package_stimulus_set(proto_stimulus_set):
    """

    :param proto_stimulus_set:
    :return:
    """
    # * Create a zip file of the images (respecting image_path_within_store) and upload it to S3
    # * Add StimulusSet lookup data to the lookup database
    # * Add per-image metadata to the lookup database
    pass




