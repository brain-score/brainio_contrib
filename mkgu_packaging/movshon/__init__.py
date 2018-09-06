import os
import re
from glob import glob

import h5py
import numpy as np
import pandas as pd
import xarray as xr


def load_stimuli(data_path):
    integer_fields = ['family', 'sample']
    stimuli_directory = os.path.join(data_path, 'stim')
    stimuli = []
    for filepath in glob(f"{stimuli_directory}/*.png"):
        # sample filename: noise-320x320-im13-smp8
        filename = os.path.basename(filepath)
        pattern = "^(?P<type>[^-]+)-(?P<resolution>[^-]+)-im(?P<family>[0-9]*)-smp(?P<sample>[0-9]+)\.png$"
        match = re.match(pattern, filename)
        assert match
        fields = match.groupdict()
        fields = {field: value if field not in integer_fields else int(value) for field, value in fields.items()}
        stimuli.append({**fields, **{'filepath': filepath, 'filename': filename}})

    stimuli = pd.DataFrame(stimuli)
    assert len(stimuli) == 15 * 2 * 15
    assert len(np.unique(stimuli['family'])) == 15
    assert len(np.unique(stimuli['type'])) == 2
    assert len(np.unique(stimuli['sample'])) == 15
    return stimuli


def load_responses(data_path):
    # from the readme.m: data is in the form:
    # (texFamily) x (texType) x (sample) x (rep) x (timeBin) x (cellNum)
    # (15)        x (2)       x (15)     x (20)  x (300)     x (102+)
    # in python, ordering is inverted:
    # (cellNum) x (timeBin) x (rep) x (sample) x (texType) x (texFamily)
    # (102+)    x (300)     x (20)  x (15)     x (2)       x (15)
    responses = h5py.File(os.path.join(data_path, 'data', 'FreemanZiemba2013_V1V2data.mat'), 'r')
    v1, v2 = responses['v1'], responses['v2']
    assert v1.shape[1:] == v2.shape[1:]  # same except cells
    responses = np.concatenate([v1, v2])

    assembly = xr.DataArray(responses,
                            coords={
                                'neuroid_id': list(range(responses.shape[0])),
                                'region': ('neuroid_id', ['V1'] * v1.shape[0] + ['V2'] * v2.shape[0]),
                                'time_bin': list(range(responses.shape[1])),
                                'repetition': list(range(responses.shape[2])),
                                'sample': list(range(responses.shape[3])),
                                'texture_type': list(range(responses.shape[4])),
                                'texture_family': list(range(responses.shape[5]))
                            },
                            dims=['neuroid_id', 'time_bin', 'repetition', 'sample', 'texture_type', 'texture_family'])
    assembly = assembly.stack(neuroid=['neuroid_id'])
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


def main():
    data_path = os.path.join(os.path.dirname(__file__), 'FreemanZiemba2013')

    stimuli = load_stimuli(data_path)
    responses = load_responses(data_path)

    # fix response stimulus annotations
    def build_mapping(response_key, stimuli_key):
        assert len(np.unique(responses[response_key])) == len(np.unique(stimuli[stimuli_key]))
        return {response_family: stimuli_family for response_family, stimuli_family in
                zip(np.unique(responses[response_key]), np.unique(stimuli[stimuli_key]))}

    mapping_keys = {'texture_family': 'family', 'texture_type': 'type', 'sample': 'sample'}
    assert np.prod([shape for shape, include in
                    zip(responses.shape, [dim in mapping_keys for dim in responses.dims]) if include]) \
           == len(stimuli), "number of stimuli do not match"
    mappings = {response_key: build_mapping(response_key, stimuli_key)
                for response_key, stimuli_key in mapping_keys.items()}
    coords = {coord: (dims, values if coord not in mappings else [mappings[coord][value] for value in values])
              for coord, dims, values in walk_coords(responses)}
    assembly = xr.DataArray(responses.values, coords=coords, dims=responses.dims)
    assembly = assembly.stack(presentation=['texture_type', 'texture_family', 'sample', 'repetition'])
    presentations = pd.DataFrame({key: values for key, dims, values in walk_coords(assembly['presentation'])})

    # attach filepaths
    filepaths = []
    for _, row in presentations.iterrows():
        row_filter = [stimuli[stimuli_key] == row[response_key] for response_key, stimuli_key in mapping_keys.items()]
        row_filter = np.array(row_filter).all(axis=0)
        stimuli_row = stimuli[row_filter]
        filepath = stimuli_row['filepath']
        assert len(filepath) == 1, "expected exactly one match"
        filepaths.append(filepath.values[0])
    assembly['stimuli_path'] = 'presentation', filepaths

    return assembly


if __name__ == '__main__':
    main()
