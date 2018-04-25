import glob
import os

import numpy as np
import pandas as pd
import xarray as xr


def main():
    directory = os.path.join(os.path.dirname(__file__), 'V1Data', 'NatRev')
    data_files = glob.glob(os.path.join(directory, 'data', '*.csv'))
    data = pd.concat((pd.read_csv(f) for f in data_files))
    num_duplicates = len(data) - len(data.drop_duplicates())
    assert num_duplicates == 0
    data.rename(columns={'cellName': 'neuroid', 'stimuliPaths': 'image_file_name'}, inplace=True)
    print("Found responses for {} cells, average spike count {:.4f}".format(len(np.unique(data['neuroid'])),
                                                                            np.mean(data['response'])))

    responses, neuroids, image_file_names = np.full((len(data), len(np.unique(data['neuroid']))), np.nan), [], []
    row = 0
    for neuroid_iter, neuroid in enumerate(np.unique(data['neuroid'])):
        neuroid_data = data[data['neuroid'] == neuroid]
        responses[row:row + len(neuroid_data), neuroid_iter] = neuroid_data['response'].values
        row += len(neuroid_data)
        neuroids.append(neuroid)
        image_file_names += neuroid_data['image_file_name'].values.tolist()

    assembly = xr.DataArray(responses,
                            coords={'image_file_name': data['image_file_name'], 'neuroid': np.unique(data['neuroid'])},
                            dims=['image_file_name', 'neuroid'])
    print("Created {} assembly".format(" x ".join(map(str, assembly.shape))))
    savepath = os.path.abspath(os.path.join(directory, 'data.nc'))
    assembly.to_netcdf(savepath)
    print("Saved to {}".format(savepath))


if __name__ == '__main__':
    main()
