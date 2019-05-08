import time
from pathlib import Path
import pandas as pd
import brainio_contrib
from brainio_contrib.packaging import package_stimulus_set, add_image_metadata_to_db
from brainio_collection.lookup import pwdb
from brainio_collection.stimuli import StimulusSetModel, ImageStoreModel, AttributeModel, ImageModel, \
    StimulusSetImageMap, ImageStoreMap, ImageMetaModel


def test_package_stimulus_set():
    proto = pd.read_csv("images/test_images.csv")
    stim = package_stimulus_set(proto)
    assert stim


def test_add_image_metadata_to_db():
    pwdb.connect(reuse_if_open=True)
    csv_path = Path(__file__).parent / "images" / "test_images.csv"
    proto = pd.read_csv(csv_path)
    stim_set_model, created = StimulusSetModel.get_or_create(name="test")
    image_store_model, created = ImageStoreModel.get_or_create(location_type="test_loc_type", store_type="test_store_type",
                                                               location="test_loc", unique_name="test_store",
                                                               sha1=f"foo.{time.time()}")
    add_image_metadata_to_db(proto, stim_set_model, image_store_model)
    pw_query = ImageModel.select() \
        .join(StimulusSetImageMap) \
        .join(StimulusSetModel) \
        .where(StimulusSetModel.name == stim_set_model.name)
    print(f"Length of select query:  {len(pw_query)}")
    assert len(pw_query) == 25


