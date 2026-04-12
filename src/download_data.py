import shutil, zipfile, kagglehub
from pathlib import Path
from src.config.paths import RAW_DIR

def download_data(dataset_path: str, force: bool = False):
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    has_data = any(f for f in RAW_DIR.iterdir() if f.is_file() and not f.name.startswith('.'))

    if not force and has_data:
        print(f"Data exists in {RAW_DIR}. Skipping.")
        return

    print(f"Downloading {dataset_path}...")
    downloaded_path = Path(kagglehub.dataset_download(dataset_path, force_download=force))

    if downloaded_path.suffix == ".zip":
        with zipfile.ZipFile(downloaded_path, "r") as z:
            z.extractall(RAW_DIR)
    elif downloaded_path.is_dir():
        for f in downloaded_path.iterdir():
            if f.is_file(): shutil.copy(f, RAW_DIR)
    else:
        shutil.copy(downloaded_path, RAW_DIR)

    print(f"Files saved to: {RAW_DIR}")
