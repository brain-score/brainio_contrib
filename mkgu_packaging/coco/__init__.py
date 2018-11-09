import os
import re
from glob import glob

import h5py
import numpy as np
import pandas as pd
from brainscore.knownfile import KnownFile as kf
from result_caching import store
from tqdm import tqdm


@store(identifier_ignore=['stimuli_directory'])
def collect_stimuli(stimuli_directory):
    meta = os.path.join(stimuli_directory, 'cocogray_labels.mat')
    meta = h5py.File(meta, 'r')
    labels = [''.join(chr(c) for c in meta[meta['lb'].value[0, i]]) for i in range(meta['lb'].value[0].size)]
    stimuli = []
    for image_file_path in tqdm(glob(os.path.join(stimuli_directory, '*.png'))):
        image_file_name = os.path.basename(image_file_path)
        image_number = re.match('im([0-9]+).png', image_file_name)
        image_number = int(image_number.group(1))
        im_kf = kf(image_file_path)
        stimuli.append({
            'image_id': im_kf.sha1,
            'image_file_name': image_file_name,
            'image_file_path': image_file_path,
            'image_file_sha1': im_kf.sha1,
            'image_number': image_number,
            'image_store_path': f"coco-gray/{image_file_name}",
            'label': labels[image_number],
        })
    stimuli = pd.DataFrame(stimuli)
    assert len(stimuli) == 1600
    assert len(np.unique(stimuli['image_id'])) == len(stimuli)
    return stimuli


def main():
    stimuli_dir = os.path.join(os.path.dirname(__file__), 'stimuli')
    stimuli = collect_stimuli(stimuli_dir)

    return stimuli


if __name__ == '__main__':
    main()
