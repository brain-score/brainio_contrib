# BrainIO contrib

Contains packaging scripts that generate the StimulusSets and assemblies 
in https://github.com/brain-score/brainio_collection.

The scripts herein are snapshots of code that added contents to brainio_collection.
Since they are only executed once, they are not maintained.
To re-package stimuli or assemblies, it is thus necessary to revert to the commit of the packaging script,
packaging scripts are **not** updated to run in future versions of this codebase.
Conversely, updates to e.g. the automated packaging functions should **not** update old packaging scripts.

## Dependencies
We try to keep the dependencies in this repository minimal.
If a packaging script requires more dependencies, add a `requirements.txt` file in the respective package.

## Getting started

### Installation

```shell script
git clone https://github.com/brain-score/brainio_contrib.git
cd brainio_contrib

python setup.py install
```

### Contributing stimuli

The stimuli and its associated metadata need to be made into a `StimulusSet`, which is essentially just a table with all the information. The `StimulusSet` needs to have an `image_id` column, a field `image_paths` which maps `image_id` to your the local path of the image, and a field `identifier` with the name your stimulus set.

```python
from pandas import DataFrame
from brainio_base.stimuli import StimulusSet


stimulus_set = StimulusSet(DataFrame({'image_id': [0, 1]}))
file_paths = ['/project/video_0.mp4', '/project/video_1.mp4']

stimulus_set.image_paths = {stimulus_set.at[idx, 'image_id']: file_paths[idx] for idx in range(len(stimulus_set))}
stimulus_set.identifier = 'dicarlo.Project2021'

```

Once the stimulus set is ready, you can push it to BrainIO. There are three locations you can send it to:

1. `brainio.dicarlo`: For DiCarlo Lab stimulus sets
2. `brainio.contrib`: For external stimulus sets
3. `brainio.requested`: For to-be-run-on-monkey-machine stimulus sets

Choose the one most appropriate, and call the `package_stimulus_set` function.

```python
from brainio_collection.packaging import package_stimulus_set

package_stimulus_set(stimulus_set, stimulus_set_identifier=stimulus_set.identifier, bucket_name='brainio.requested')
```

Please note that if you're submitting stimuli to the `brainio.requested` bucket, your images need to be named in particular format:
- They need to be sequentially numbered
- Don't use leading zeros ('001.png' is not valid)
- Examples of valid names are 'image_1.png', '1.png', 'Nat300_1.png', etc.

Once you're done, please create a [Pull Request](https://github.com/brain-score/brainio_contrib/pulls) to merge your code.
