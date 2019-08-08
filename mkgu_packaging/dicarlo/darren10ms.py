from dldata.stimulus_sets import hvm


def main():
    '''Packages the DataArray (stimulus set the same as HvM). Returns an xarray of ["neuroid", "presentation", "time_bin"]
    Note: using my "10ms" branch of dldata'''
    dataset = hvm.HvMWithDiscfade()
    xr = dataset.xr_from_hvm_10ms_temporal()


if __name__ == '__main__':
    main()
