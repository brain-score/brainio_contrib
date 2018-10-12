import os
import re
from glob import glob

import h5py
import numpy as np
import pandas as pd
import xarray as xr

from brainscore.knownfile import KnownFile as kf


# from FreemanZiemba2013_V1V2data_readme.m
textureNumOrder = [327, 336, 393, 402, 13, 18, 23, 30, 38, 48, 52, 56, 60, 71, 99]


def load_stimuli(stimuli_directory):
    stimuli = []
    for filepath in glob(f"{stimuli_directory}/*.png"):
        filename = os.path.basename(filepath)
        fields = fields_from_image_name(filename)
        stimuli.append({**fields, **{'filepath': filepath, 'filename': filename}})

    stimuli = pd.DataFrame(stimuli)
    assert len(stimuli) == 15 * 2 * 15
    assert len(np.unique(stimuli['texture_family'])) == 15
    assert len(np.unique(stimuli['texture_type'])) == 2
    assert len(np.unique(stimuli['sample'])) == 15
    return stimuli


def load_responses(response_file, stimuli_directory):
    # from the readme.m: data is in the form:
    # (texFamily) x (texType) x (sample) x (rep) x (timeBin) x (cellNum)
    # (15)        x (2)       x (15)     x (20)  x (300)     x (102+)
    # in python, ordering is inverted:
    # (cellNum) x (timeBin) x (rep) x (sample) x (texType) x (texFamily)
    # (102+)    x (300)     x (20)  x (15)     x (2)       x (15)
    responses = h5py.File(response_file, 'r')
    v1, v2 = responses['v1'], responses['v2']
    assert v1.shape[1:] == v2.shape[1:]  # same except cells
    responses = np.concatenate([v1, v2])

    assembly = xr.DataArray(responses,
                            coords={
                                'neuroid_id': ("neuroid", list(range(1, responses.shape[0] + 1))),
                                'region': ('neuroid', ['V1'] * v1.shape[0] + ['V2'] * v2.shape[0]),
                                'time_bin_start': ("time_bin", list(range(responses.shape[1]))),  # each bin is 1 ms
                                'time_bin_end': ("time_bin", list(range(1, responses.shape[1] + 1))),
                                'repetition': list(range(responses.shape[2])),
                                'sample': list(range(1, responses.shape[3] + 1)),
                                'texture_type': ["noise", "texture"],
                                'texture_family': textureNumOrder
                            },
                            dims=['neuroid', 'time_bin', 'repetition', 'sample', 'texture_type', 'texture_family'])

    assembly = assembly.stack(presentation=['texture_type', 'texture_family', 'sample', 'repetition'])

    image_fields = zip(*[assembly[k].values for k in ['texture_type', 'texture_family', 'sample']])
    image_names = [image_name_from_fields(im[0], "320x320", im[1], im[2]) for im in image_fields]
    assembly["image_file_name"] = ("presentation", image_names)

    kfs = {}
    sha1s = []
    for image_name in image_names:
        if image_name in kfs:
            im_kf = kfs[image_name]
        else:
            im_kf = kf(os.path.join(stimuli_directory, image_name))
            kfs[image_name] = im_kf
        sha1s.append(im_kf.sha1)
    assembly["image_id"] = ("presentation", sha1s)

    return assembly


def walk_coords(assembly):  # from brain-score/brainscore/assemblies.py
    """
    walks through coords and all levels, just like the `__repr__` function, yielding `(name, dims, values)`.
    """
    coords = {}

    for name, values in assembly.coords.items():
        # partly borrowed from xarray.core.formatting#summarize_coord
        is_index = name in assembly.dims
        if is_index and values.variable.level_names:
            for level in values.variable.level_names:
                level_values = assembly.coords[level]
                yield level, level_values.dims, level_values.values
        else:
            yield name, values.dims, values.values
    return coords


def image_name_from_fields(texture_type, resolution, texture_family, sample):
    mapping = {"noise": "noise", "texture": "tex"}
    return f"{mapping[texture_type]}-{resolution}-im{int(texture_family)}-smp{int(sample)}.png"


def fields_from_image_name(image_name):
    # sample filename: noise-320x320-im13-smp8
    integer_fields = ['family', 'sample']
    mapping = {"noise": "noise", "tex": "texture"}
    pattern = "^(?P<texture_type>[^-]+)-(?P<resolution>[^-]+)-im(?P<texture_family>[0-9]*)-smp(?P<sample>[0-9]+)\.png$"
    match = re.match(pattern, image_name)
    assert match
    fields = match.groupdict()
    fields = {field: value if field not in integer_fields else int(value) for field, value in fields.items()}
    fields = {field: value if field != "texture_type" else mapping[value] for field, value in fields.items()}
    return fields


def main():
    data_path = os.path.join(os.path.dirname(__file__), 'FreemanZiemba2013')
    stimuli_directory = os.path.join(data_path, 'stim')
    response_file = os.path.join(data_path, 'data', 'FreemanZiemba2013_V1V2data.mat')

    stimuli = load_stimuli(stimuli_directory)
    assembly = load_responses(response_file, stimuli_directory)

    print(assembly)
    nonzero = np.count_nonzero(assembly)
    print(nonzero)
    assert nonzero > 0


if __name__ == '__main__':
    main()
