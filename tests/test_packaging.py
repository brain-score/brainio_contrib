import time
from pathlib import Path
import pandas as pd
import brainio_contrib
from brainio_contrib.packaging import package_stimulus_set, add_image_metadata_to_db
from brainio_collection.lookup import pwdb
from brainio_collection.stimuli import StimulusSetModel, ImageStoreModel, AttributeModel, ImageModel, \
    StimulusSetImageMap, ImageStoreMap, ImageMetaModel


def test_package_stimulus_set():
    proto = prep_proto_stim()
    stim = package_stimulus_set(proto)
    assert stim


def test_add_image_metadata_to_db():
    pwdb.connect(reuse_if_open=True)
    proto = prep_proto_stim()
    stim_set_model, created = StimulusSetModel.get_or_create(name=f"test_stimulus_set.{time.time()}")
    image_store_model, created = ImageStoreModel.get_or_create(location_type="test_loc_type", store_type="test_store_type",
                                                               location="test_loc", unique_name=f"test_store.{time.time()}",
                                                               sha1=f"foo.{time.time()}")
    add_image_metadata_to_db(proto, stim_set_model, image_store_model)
    pw_query = ImageModel.select() \
        .join(StimulusSetImageMap) \
        .join(StimulusSetModel) \
        .where(StimulusSetModel.name == stim_set_model.name)
    print(f"Length of select query:  {len(pw_query)}")
    assert len(pw_query) == 25


def prep_proto_stim():
    image_dir = Path(__file__).parent / "images"
    csv_path = image_dir / "test_images.csv"
    proto = pd.read_csv(csv_path)
    proto["image_current_local_file_path"] = [image_dir / f for f in proto["image_current_relative_file_path"]]
    del proto["image_current_relative_file_path"]
    return proto
