import os
from pathlib import Path
import json

import numpy as np
import xarray as xr
import pandas as pd

import brainio_collection
from brainio_base.assemblies import NeuroidAssembly
from brainio_contrib.packaging import package_data_assembly
from mkgu_packaging.dicarlo.sanghavi import filter_neuroids


def load_responses(data_dir, stimuli):
    psth = np.load(data_dir / 'solo.rsvp.hvm.experiment_psth.npy')  # Shaped images x repetitions x time_bins x channels

    # Drop first (index 0) and second last session (index 25) since they had only one repetition each
    # Actually not, since we're sticking to older protocol re: data cleaning for now
    # psth = np.delete(psth, (0, 25), axis=1)

    # Select responses from 70-170ms (accounting for ~30ms delay recorded from photodiode signal) and take average
    timebase = np.arange(-100, 381, 10)  # PSTH from -100ms to 380ms relative to stimulus onset
    assert len(timebase) == psth.shape[2]
    t_cols = np.where((timebase >= 100) & (timebase < 200))[0]  # Delay recorded on photodiode is ~30ms, so adding that
    rate = np.mean(psth[:, :, t_cols, :], axis=2)  # Shaped images x repetitions x channels

    # Load image related meta data (id ordering differs from dicarlo.hvm)
    image_id = [x.split()[0][:-4] for x in open(data_dir.parent / 'image-metadata' / 'hvm_map.txt').readlines()]
    # Load neuroid related meta data
    neuroid_meta = pd.DataFrame(json.load(open(data_dir.parent / 'array-metadata' / 'mapping.json')))

    assembly = xr.DataArray(rate,
                            coords={'repetition': ('repetition', list(range(rate.shape[1]))),
                                    'image_id': ('image', image_id)},
                            dims=['image', 'repetition', 'neuroid'])

    for column_name, column_data in neuroid_meta.iteritems():
        assembly = assembly.assign_coords(**{f'{column_name}': ('neuroid', list(column_data.values))})

    assembly = assembly.sortby(assembly.image_id)
    stimuli = stimuli.sort_values(by='image_id').reset_index(drop=True)
    for column_name, column_data in stimuli.iteritems():
        assembly = assembly.assign_coords(**{f'{column_name}': ('image', list(column_data.values))})
    assembly = assembly.sortby(assembly.id)  # Re-order by id to match dicarlo.hvm ordering

    # Collapse dimensions 'image' and 'repetitions' into a single 'presentation' dimension
    assembly = assembly.stack(presentation=('image', 'repetition')).reset_index('presentation')
    assembly = NeuroidAssembly(assembly)

    # Filter noisy electrodes
    assembly = filter_neuroids(assembly, 0.7)

    # Add time info
    assembly = assembly.expand_dims('time_bin')
    assembly['time_bin_start'] = 'time_bin', [70]
    assembly['time_bin_end'] = 'time_bin', [170]
    assembly = assembly.transpose('presentation', 'neuroid', 'time_bin')

    # Add other experiment and data processing related info
    assembly.attrs['experiment_paradigm'] = 'rsvp'
    assembly.attrs['sampling_rate_hz'] = 20000
    assembly.attrs['image_size_degree'] = 8
    assembly.attrs['stim_on_time_ms'] = 100
    assembly.attrs['stim_off_time_ms'] = 100
    assembly.attrs['stim_on_delay_ms'] = 0
    assembly.attrs['inter_trial_interval_ms'] = 500
    assembly.attrs['fixation_point_size_degree'] = 0.2
    assembly.attrs['fixation_window_size_degree'] = 2
    assembly.attrs['threshold_sd'] = 3
    assembly.attrs['chunks_for_threshold'] = 10
    assembly.attrs['passband_hz'] = [300, 6000]
    assembly.attrs['ellip_order'] = 2

    return assembly


def main():
    data_dir = Path(__file__).parents[6] / 'data2' / 'active' / 'users' / 'sachis' / 'database'
    assert os.path.isdir(data_dir)

    stimuli = brainio_collection.get_stimulus_set('dicarlo.hvm')
    assembly = load_responses(data_dir, stimuli)
    assembly.name = 'dicarlo.Sanghavi2020'
    print(assembly)

    print('Packaging assembly')
    package_data_assembly(assembly, data_assembly_name=assembly.name, stimulus_set_name=stimuli.name,
                          bucket_name="brainio-dicarlo")
    return


if __name__ == '__main__':
    main()
