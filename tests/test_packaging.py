import os
import zipfile

import datetime
import pandas as pd
import pytest
from pathlib import Path

from brainio_base.stimuli import StimulusSet

import brainio_collection
from brainio_collection.knownfile import KnownFile as kf
from brainio_collection.lookup import pwdb
from brainio_collection.stimuli import StimulusSetModel, ImageStoreModel, ImageModel, \
    StimulusSetImageMap, ImageStoreMap
from brainio_contrib.packaging import package_stimulus_set, add_image_metadata_to_db, create_image_zip, \
    add_stimulus_set_metadata_and_lookup_to_db


@pytest.fixture
def transaction():
    with pwdb.atomic() as txn:
        yield txn
        txn.rollback()


def now():
    return datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")


def prep_proto_stim():
    image_dir = Path(__file__).parent / "images"
    csv_path = image_dir / "test_images.csv"
    proto = pd.read_csv(csv_path)
    proto["image_id"] = [f"{iid}.{now()}" for iid in proto["image_id"]]
    proto[f"test_{now()}"] = [f"{iid}.{now()}" for iid in proto["image_id"]]
    proto = StimulusSet(proto)
    proto.image_paths = {row.image_id: image_dir / row.image_current_relative_file_path for row in proto.itertuples()}
    proto['image_file_name']= proto['image_path_within_store']
    return proto


def test_create_image_zip():
    target_zip_path = Path(__file__).parent / "test_images.zip"
    proto = prep_proto_stim()
    sha1 = create_image_zip(proto, target_zip_path)
    with zipfile.ZipFile(target_zip_path, "r") as target_zip:
        infolist = target_zip.infolist()
        assert len(infolist) == 25
        for zi in infolist:
            print(zi.filename)
            print(len(zi.filename))
            assert zi.filename.endswith(".png")
            assert not zi.is_dir()
            assert len(zi.filename) == 44


def test_add_image_metadata_to_db(transaction):
    pwdb.connect(reuse_if_open=True)
    proto = prep_proto_stim()
    stim_set_model, created = StimulusSetModel.get_or_create(name=f"test_stimulus_set.{now()}")
    image_store_model, created = ImageStoreModel.get_or_create(location_type="test_loc_type", store_type="test_store_type",
                                                               location="test_loc", unique_name=f"test_store.{now()}",
                                                               sha1=f"foo.{now()}")
    add_image_metadata_to_db(proto, stim_set_model, image_store_model)
    pw_query = ImageModel.select() \
        .join(StimulusSetImageMap) \
        .join(StimulusSetModel) \
        .where(StimulusSetModel.name == stim_set_model.name)
    print(f"Length of select query:  {len(pw_query)}")
    assert len(pw_query) == 25


def test_add_stimulus_set_metadata_and_lookup_to_db(transaction):
    stim_set_name = f"test_stimulus_set.{now()}"
    bucket_name = "brainio-temp"
    zip_file_name = "test_images.zip"
    image_store_unique_name = f"test_store.{now()}"
    target_zip_path = Path(__file__).parent / zip_file_name
    proto = prep_proto_stim()
    sha1 = create_image_zip(proto, target_zip_path)
    stim_set_model = add_stimulus_set_metadata_and_lookup_to_db(proto, stim_set_name, bucket_name,
                                                                zip_file_name, image_store_unique_name,
                                                                sha1)
    pw_query = ImageStoreModel.select() \
        .join(ImageStoreMap) \
        .join(ImageModel) \
        .join(StimulusSetImageMap) \
        .join(StimulusSetModel) \
        .where(StimulusSetModel.name == stim_set_model.name)
    assert len(pw_query) == 25


@pytest.mark.private_access
def test_package_stimulus_set(transaction):
    proto = prep_proto_stim()
    stim_set_name = "dicarlo.test." + now()
    test_bucket = "brainio-temp"
    stim_model = package_stimulus_set(proto, stimulus_set_name=stim_set_name, bucket_name=test_bucket)
    assert stim_model
    assert stim_model.name == stim_set_name
    stim_set_fetched = brainio_collection.get_stimulus_set(stim_set_name)
    assert len(proto) == len(stim_set_fetched)
    for image in proto.itertuples():
        orig = proto.get_image(image.image_id)
        fetched = stim_set_fetched.get_image(image.image_id)
        assert os.path.basename(orig) == os.path.basename(fetched)
        kf_orig = kf(orig)
        kf_fetched = kf(fetched)
        assert kf_orig.sha1 == kf_fetched.sha1



