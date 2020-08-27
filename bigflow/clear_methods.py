from pathlib import Path
import shutil


def clear_image_leftovers(image_dir: Path):
    print(f'Removing: {str(image_dir.absolute())}')
    shutil.rmtree(image_dir, ignore_errors=True)


def clear_dags_leftovers(dags_dir: Path):
    print(f'Removing: {str(dags_dir.absolute())}')
    shutil.rmtree(dags_dir, ignore_errors=True)


def clear_package_leftovers(dist_dir: Path, eggs_dir: Path, build_dir: Path):
    for to_delete in [build_dir, dist_dir, eggs_dir]:
        print(f'Removing: {str(to_delete.absolute())}')
        shutil.rmtree(to_delete, ignore_errors=True)