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
