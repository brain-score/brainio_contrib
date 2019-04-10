import os
from glob import glob

import pandas as pd
import xarray as xr

from brainio_base.assemblies import BehavioralAssembly
from brainio_base.stimuli import StimulusSet


def get_objectome():
    data_path = os.path.join(os.path.dirname(__file__), 'Rajalingham2018Objectome', 'data')
    objectome = pd.read_pickle(os.path.join(data_path, 'objectome24s100_humanpool.pkl'))
    objectome['correct'] = objectome['choice'] == objectome['sample_obj']
    objectome['truth'] = objectome['sample_obj']

    subsample = pd.read_pickle(os.path.join(data_path, 'objectome24s100_imgsubsampled240_pandas.pkl'))
    objectome['enough_human_data'] = objectome['id'].isin(subsample.values[:, 0])
    objectome = to_xarray(objectome)
    return objectome


def to_xarray(objectome):
    columns = objectome.columns
    objectome = xr.DataArray(objectome['choice'],
                             coords={column: ('presentation', objectome[column]) for column in columns},
                             dims=['presentation'])
    objectome = objectome.rename({'id': 'image_id'})
    objectome = objectome.set_index(presentation=[col if col != 'id' else 'image_id' for col in columns])
    objectome = BehavioralAssembly(objectome)
    return objectome


def load_stimuli():
    # cp /braintree/home/msch/brain-score_packaging/objectome/objectome-224/* ./Rajalingham2018Objectome/stim/
    stim_path = os.path.join(os.path.dirname(__file__), 'Rajalingham2018Objectome', 'stim')
    stimuli_paths = list(glob(os.path.join(stim_path, '*.png')))
    stimuli = StimulusSet({'filepath': stimuli_paths,
                           'image_id': [os.path.splitext(os.path.basename(filepath))[0] for filepath in stimuli_paths]})
    return stimuli


def load_responses():
    # cp /braintree/home/msch/brain-score_packaging/objectome/data/* ./Rajalingham2018Objectome/data/
    objectome = get_objectome()
    objectome.name = 'dicarlo.Rajalingham2018'
    fitting_objectome, testing_objectome = objectome.sel(enough_human_data=False), objectome.sel(enough_human_data=True)
    fitting_objectome.name += '.partial_trials'
    testing_objectome.name += '.full_trials'
    return objectome, fitting_objectome, testing_objectome


def main():
    all_stimuli = load_stimuli()
    [all_assembly, public_assembly, private_assembly] = load_responses()
    public_stimuli = all_stimuli[all_stimuli['image_id'].isin(public_assembly['image_id'].values)]
    private_stimuli = all_stimuli[all_stimuli['image_id'].isin(private_assembly['image_id'].values)]
    public_stimuli.name, private_stimuli.name = public_assembly.name, private_assembly.name

    assert len(public_assembly) + len(private_assembly) == len(all_assembly) == 927296
    assert len(private_assembly) == 341785
    assert len(set(public_assembly['image_id'].values)) == len(public_stimuli) == 2160
    assert len(set(private_assembly['image_id'].values)) == len(private_stimuli) == 240
    assert set(all_stimuli['image_id'].values) == set(all_assembly['image_id'].values)
    assert set(public_stimuli['image_id'].values) == set(public_assembly['image_id'].values)
    assert set(private_stimuli['image_id'].values) == set(private_assembly['image_id'].values)
    assert len(set(private_assembly['choice'].values)) == len(set(public_assembly['choice'].values)) == 24

    print([assembly.name for assembly in [all_assembly, public_assembly, private_assembly]])
    return [(all_assembly, all_stimuli), (public_assembly, public_stimuli), (private_assembly, private_stimuli)]


if __name__ == '__main__':
    main()
