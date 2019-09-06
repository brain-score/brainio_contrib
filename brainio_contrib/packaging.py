import logging
import os
import zipfile

import boto3
from pathlib import Path
from tqdm import tqdm

from brainio_collection.assemblies import AssemblyModel, AssemblyStoreModel, AssemblyStoreMap
from brainio_collection.knownfile import KnownFile as kf
from brainio_collection.lookup import pwdb
from brainio_collection.stimuli import StimulusSetModel, ImageStoreModel, AttributeModel, ImageModel, \
    StimulusSetImageMap, ImageStoreMap, ImageMetaModel

_logger = logging.getLogger(__name__)


def create_image_zip(proto_stimulus_set, target_zip_path):
    _logger.debug(f"Zipping stimulus set to {target_zip_path}")
    os.makedirs(os.path.dirname(target_zip_path), exist_ok=True)
    with zipfile.ZipFile(target_zip_path, 'w') as target_zip:
        for image in proto_stimulus_set.itertuples():
            target_zip.write(image.image_current_local_file_path, arcname=image.image_path_within_store)
    zip_kf = kf(target_zip_path)
    return zip_kf.sha1


def upload_to_s3(source_file_path, bucket_name, target_s3_key):
    _logger.debug(f"Uploading {source_file_path} to {bucket_name}/{target_s3_key}")

    file_size = os.path.getsize(source_file_path)
    with tqdm(total=file_size, unit='B', unit_scale=True, desc="upload to s3") as progress_bar:
        def progress_hook(bytes_amount):
            if bytes_amount > 0:  # at the end, this sometimes passes a negative byte amount which tqdm can't handle
                progress_bar.update(bytes_amount)

        client = boto3.client('s3')
        client.upload_file(str(source_file_path), bucket_name, target_s3_key, Callback=progress_hook)


def extract_specific(proto_stimulus_set):
    general = ['image_current_local_file_path', 'image_id', 'image_path_within_store']
    stimulus_set_specific_attributes = []
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

    for image in tqdm(proto_stimulus_set.itertuples(), desc='images->db', total=len(proto_stimulus_set)):
        pw_image, created = ImageModel.get_or_create(image_id=image.image_id)
        StimulusSetImageMap.get_or_create(stimulus_set=stim_set_model, image=pw_image)
        ImageStoreMap.get_or_create(image=pw_image, image_store=image_store_model, path=image.image_path_within_store)
        for name in eav_attributes:
            ImageMetaModel.get_or_create(image=pw_image, attribute=eav_attributes[name],
                                         value=str(getattr(image, name)))


def add_stimulus_set_lookup_to_db(stimulus_set_name, bucket_name, zip_file_name,
                                  image_store_unique_name, zip_sha1):
    _logger.debug(f"Adding stimulus set {stimulus_set_name} to db")
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


def package_stimulus_set(proto_stimulus_set, stimulus_set_name, bucket_name="brainio-contrib"):
    """
    Package a set of images along with their metadata for the BrainIO system.
    :param proto_stimulus_set: A pandas DataFrame containing one row for each image, and the columns ['image_current_local_file_path', 'image_id', 'image_path_within_store'] and columns for all stimulus-set-specific metadata
    :param stimulus_set_name: A dot-separated string starting with a lab identifier.
    :param bucket_name: 'brainio-dicarlo' for DiCarlo Lab stimulus sets, 'brainio-contrib' for
    external stimulus sets.
    :return: A peewee ORM StimulusSetModel.
    """
    image_store_unique_name = "image_" + stimulus_set_name.replace(".", "_")
    zip_file_name = image_store_unique_name + ".zip"
    target_zip_path = Path(__file__).parent / zip_file_name
    s3_key = zip_file_name

    sha1 = create_image_zip(proto_stimulus_set, str(target_zip_path))
    upload_to_s3(str(target_zip_path), bucket_name, s3_key)
    stimulus_set_model = add_stimulus_set_metadata_and_lookup_to_db(proto_stimulus_set, stimulus_set_name, bucket_name,
                                                                    zip_file_name, image_store_unique_name, sha1)
    return stimulus_set_model


def write_netcdf(assembly, target_netcdf_file):
    _logger.debug(f"Writing assembly to {target_netcdf_file}")
    assembly.reset_index(assembly.indexes.keys(), inplace=True)
    assembly.to_netcdf(target_netcdf_file)
    netcdf_kf = kf(target_netcdf_file)
    return netcdf_kf.sha1


def add_data_assembly_lookup_to_db(assembly_name, stim_set_model, bucket_name, netcdf_sha1,
                                   assembly_store_unique_name, s3_key, assembly_class="NeuronRecordingAssembly"):
    _logger.debug(f"Adding assembly {assembly_name} to db")
    assy, created = AssemblyModel.get_or_create(name=assembly_name, assembly_class=assembly_class,
                                                stimulus_set=stim_set_model)
    store, created = AssemblyStoreModel.get_or_create(assembly_type="netCDF",
                                                      location_type="S3",
                                                      location=f"https://{bucket_name}.s3.amazonaws.com/{s3_key}",
                                                      unique_name=assembly_store_unique_name,
                                                      sha1=netcdf_sha1)
    assy_store_map, created = AssemblyStoreMap.get_or_create(assembly_model=assy, assembly_store_model=store,
                                                             role=assembly_name)
    return assy


def package_data_assembly(proto_data_assembly, data_assembly_name, stimulus_set_name,
                          assembly_class="NeuronRecordingAssembly", bucket_name="brainio-contrib"):
    """
    Package a set of data along with its metadata for the BrainIO system.
    :param proto_data_assembly: An xarray DataArray containing experimental measurements and all related metadata.
        * The dimensions of the DataArray (except for behavior) must be
            * neuroid
            * presentation
            * time_bin
        * The neuroid dimension must have a neuroid_id coordinate and should have coordinates for as much neural metadata as possible (e.g. region, subregion, animal, row in array, column in array, etc.)
        * The presentation dimension must have an image_id coordinate and should have coordinates for presentation-level metadata such as repetition and image_id.  The presentation dimension should not have coordinates for image-specific metadata, these will be drawn from the StimulusSet based on image_id.
        * The time_bin dimension should have coordinates time_bin_start and time_bin_end.
    :param data_assembly_name: A dot-separated string starting with a lab identifier.
        * For requests: <lab identifier>.<b for behavioral|n for neuroidal>.<m for monkey|h for human>.<proposer e.g. 'Margalit'>.<pull request number>
        * For published: <lab identifier>.<b for behavioral|n for neuroidal>.<m for monkey|h for human>.<first author e.g. 'Rajalingham'><YYYY year of publication>
    :param stimulus_set_name: The unique name of an existing StimulusSet in the BrainIO system.
    :param assembly_class: The name of a DataAssembly subclass.
    :param bucket_name: 'brainio-dicarlo' for DiCarlo Lab stimulus sets, 'brainio-contrib' for
    external stimulus sets.
    :return:
    """
    assembly_store_unique_name = "assy_" + data_assembly_name.replace(".", "_")
    netcdf_file_name = assembly_store_unique_name + ".nc"
    target_netcdf_path = Path(__file__).parent / netcdf_file_name
    s3_key = netcdf_file_name

    netcdf_kf_sha1 = write_netcdf(proto_data_assembly, target_netcdf_path)
    upload_to_s3(target_netcdf_path, bucket_name, s3_key)
    stim_set_model = StimulusSetModel.get(StimulusSetModel.name == stimulus_set_name)

    assy_model = add_data_assembly_lookup_to_db(data_assembly_name, stim_set_model, bucket_name, netcdf_kf_sha1,
                                                assembly_store_unique_name, s3_key, assembly_class)
    return assy_model
