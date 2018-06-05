import glob
import os
import re
import string

import numpy as np
import pandas as pd
import xarray as xr
import mkgu
import mkgu.assemblies
from mkgu.knownfile import KnownFile as kf


def align_debug():
    v2_base_path = "/braintree/data2/active/users/jjpr/mkgu_packaging/crcns/v2-1"
    nc_files = sorted(glob.glob(os.path.join(v2_base_path, "*/*/*.nc"), recursive=True))
    gd_arrays = []
    nonzeros_raw = []
    for f in (nc_files[0], nc_files[5]):
        print(f)
        gd_array = xr.open_dataarray(f)
        # gd_array = gd_array.T.rename({"image_file_name": "presentation"})
        # gd_array.coords["presentation_id"] = ("presentation", range(gd_array.shape[1]))
        # gd_array = gd_array.rename({"image_file_name": "presentation"})
        # gd_array.coords["presentation_id"] = ("presentation", range(gd_array.shape[0]))
        gd_array.coords["presentation_id"] = ("image_file_name", range(gd_array.shape[0]))
        # gd_array.coords["neuroid_id"] = ("neuroid", gd_array["neuroid"].values)
        # df_massage = pd.DataFrame(list(map(massage_file_name, gd_array["presentation"].values)))
        # for column in df_massage.columns:
        #     gd_array.coords[column] = ("presentation", df_massage[column])
        # gd_array.reset_index(["neuroid", "presentation"], drop=True, inplace=True)
        gd_array.reset_index("category_name", drop=True, inplace=True)
        mkgu.assemblies.gather_indexes(gd_array)
        gd_arrays.append(gd_array)
        nonzeros_raw.append(np.nonzero(~np.isnan(gd_array)))
    print("nonzeros_raw: ")
    print(nonzeros_raw)
    align_test = xr.align(*gd_arrays, join="outer")
    nonzeros_aligned = [np.nonzero(~np.isnan(da)) for da in align_test]
    print("nonzeros_aligned: ")
    print(nonzeros_aligned)
    assert nonzeros_raw[0].shape == nonzeros_aligned[0].shape


def massage_file_name(file_name):
    split = re.split("\\\\|/", file_name)
    split = [t for t in split if t]
    relative_path = os.path.join(*split[-5:])
    full_path = os.path.join("/", *split)
    sha1 = kf(full_path).sha1
    result = {
        "image_file_path_original": relative_path,
        "image_id": sha1
    }
    return result


def align_bug_reproduce():
    dims = ("x", "y")
    coords_d = {"x": ("tens", "negative"), "y": ("lower", "upper")}

    shape6 = (150000, 10)
    data6 = np.full(shape6, np.nan, dtype="float64")
    for i in range(shape6[0]):
        data6[i, i % shape6[1]] = float(i)
    coords6 = {
        "lower": ("y", list(string.ascii_lowercase[:shape6[1]])),
        "upper": ("y", [c + string.ascii_uppercase for c in list(string.ascii_uppercase[:shape6[1]])]),
        "tens": ("x", [x * 10 for x in range(shape6[0])]),
        "negative": ("x", [str(-x+x%2) for x in range(shape6[0])])
    }
    da6 = xr.DataArray(data=data6, dims=dims, coords=coords6)
    da6_file = "xarray_align_debug_da6.nc"
    da6.to_netcdf(da6_file)
    da6_reloaded = xr.open_dataarray(da6_file)

    shape7 = (300000, 10)
    data7 = np.full(shape7, np.nan, dtype="float64")
    for i in range(shape7[0]):
        data7[i, i % shape7[1]] = float(-i)
    coords7 = {
        "lower": ("y", list(string.ascii_lowercase[shape7[1]:2 * shape7[1]])),
        "upper": ("y", [c + string.ascii_uppercase for c in list(string.ascii_uppercase[shape7[1]:2 * shape7[1]])]),
        "tens": ("x", [x * 10 for x in range(shape7[0], 2 * shape7[0])]),
        "negative": ("x", [str(-x+x%2) for x in range(shape7[0], 2 * shape7[0])])
    }
    da7 = xr.DataArray(data=data7, dims=dims, coords=coords7)
    da7_file = "xarray_align_debug_da7.nc"
    da7.to_netcdf(da7_file)
    da7_reloaded = xr.open_dataarray(da7_file)

    for da in (da6_reloaded, da7_reloaded):
        da.set_index(append=True, inplace=True, **coords_d)
    aligned = xr.align(da6_reloaded.T, da7_reloaded.T, join="outer")
    print(aligned)
    print([np.nonzero(~np.isnan(da)) for da in aligned])


def main():
    # print(xr.show_versions())
    align_debug()
    # align_bug_reproduce()

if __name__ == '__main__':
    main()


