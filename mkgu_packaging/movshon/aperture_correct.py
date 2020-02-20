"""
Method to correct images in the movshon stimulus set by adding a cosine aperture
"""

from brainio_collection import get_stimulus_set
from brainio_base.stimuli import StimulusSet

import argparse
import os
import numpy as np
import imageio
from tqdm import tqdm
import copy
from pathlib import Path


# main function should be run two times, one for each stimulus set access='public' and access='target'
# saves converted image in a new folder given by the target_dir
# returns the converted StimulusSet with the new image_paths and new stimuli_id (with -aperture added in the end)
def main(access='public', target_dir='movshon_stimuli_aperture'):
    stimuli_identifier = 'movshon.FreemanZiemba2013-' + access
    target_dir = target_dir

    stimulus_set = get_stimulus_set(stimuli_identifier)
    old_dir = stimulus_set.get_image(stimulus_set['image_id'][0]).split(os.sep)[-2]
    root_dir = stimulus_set.get_image(stimulus_set['image_id'][0]).split(os.sep+old_dir+os.sep)[0]
    target_dir = root_dir + os.sep + target_dir

    Path(target_dir).mkdir(parents=True, exist_ok=True)

    # Change here for stimuli id of the converted StimulusSet
    converted_stimuli_id = f"{stimuli_identifier}-aperture"

    image_converter = ApplyCosineAperture(target_dir=target_dir)

    converted_image_paths = {}
    for image_id in tqdm(stimulus_set['image_id'], total=len(stimulus_set), desc='apply cosine aperture'):
        converted_image_path = image_converter.convert_image(image_path=stimulus_set.get_image(image_id))
        converted_image_paths[image_id] = converted_image_path

    converted_stimuli = StimulusSet(stimulus_set.copy(deep=True))
    converted_stimuli.image_paths = converted_image_paths
    converted_stimuli.name = converted_stimuli_id
    converted_stimuli.original_paths = copy.deepcopy(stimulus_set.image_paths)

    return converted_stimuli


class ApplyCosineAperture:
    def __init__(self, target_dir):
        self._target_dir = target_dir

        self.gray_c = 128
        self.input_degrees = 4
        self.aperture_degrees = 4
        self.pos = np.array([0, 0])
        self.output_degrees = 4
        self.size_px = np.array([320, 320])

        # Image size
        px_deg = self.size_px[0] / self.input_degrees

        self.size_px_out = (self.size_px * (self.output_degrees / self.input_degrees)).astype(int)
        cnt_px = (self.pos * px_deg).astype(int)

        size_px_disp = ((self.size_px_out - self.size_px) / 2).astype(int)

        self.fill_ind = [[(size_px_disp[0] + cnt_px[0]), (size_px_disp[0] + cnt_px[0] + self.size_px[0])],
                        [(size_px_disp[1] + cnt_px[1]), (size_px_disp[1] + cnt_px[1] + self.size_px[1])]]

        # Image aperture
        a = self.aperture_degrees * px_deg / 2
        # Meshgrid with pixel coordinates
        x = (np.arange(self.size_px_out[1]) - self.size_px_out[1] / 2)
        y = (np.arange(self.size_px_out[0]) - self.size_px_out[0] / 2)
        xv, yv = np.meshgrid(x, y)
        # Raised cosine aperture
        inner_mask = (xv - cnt_px[1]) ** 2 + (yv - cnt_px[0]) ** 2 < a ** 2
        cos_mask = 1 / 2 * (1 + np.cos(np.sqrt((xv - cnt_px[1]) ** 2 + (yv - cnt_px[0]) ** 2) / a * np.pi))
        cos_mask[np.logical_not(inner_mask)] = 0

        self.cos_mask = cos_mask

    def convert_image(self, image_path):

        im = imageio.imread(image_path)
        im = im - self.gray_c * np.ones(self.size_px)
        im_template = np.zeros(self.size_px_out)

        im_template[self.fill_ind[0][0]:self.fill_ind[0][1], self.fill_ind[1][0]:self.fill_ind[1][1]] = im
        im_masked = (im_template * self.cos_mask) + self.gray_c * np.ones(self.size_px_out)

        target_path = self._target_dir + os.sep + os.path.basename(image_path)

        imageio.imwrite(target_path, np.uint8(im_masked))

        return target_path


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Convert Movshon stimuli')
  parser.add_argument('--access', dest='access', type=str,
                      help='access',
                      default='public')
  parser.add_argument('--target_dir', dest='target_dir', type=str,
                      help='Target directory',
                      default='movshon_stimuli_aperture')
  args = parser.parse_args()

  main(access=args.access, target_dir=args.target_dir)

