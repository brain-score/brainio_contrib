import os
import tarfile

import wget


def download_data(out_dir, remote_url, local_untarred_dir=None):
    filepath = os.path.join(out_dir, os.path.basename(remote_url))
    if not os.path.isfile(filepath):
        wget.download(remote_url, out=filepath)
    if local_untarred_dir and not os.path.isdir(os.path.join(out_dir, local_untarred_dir)):
        untar(filepath, out_dir=out_dir)


def untar(filepath, out_dir='.'):
    tar = tarfile.open(filepath, "r:gz")
    tar.extractall(path=out_dir)


def main():
    from mkgu_packaging.cifar.cifar10 import main as main10
    from mkgu_packaging.cifar.cifar100 import main as main100
    main10()
    main100()


if __name__ == '__main__':
    main()
