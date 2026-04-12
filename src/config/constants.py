import pandas as pd

CURRENT_YEAR = pd.Timestamp.now().year
ISBN_REGEX = r"[\dX\-]{8,13}"
ASIN_REGEX = r"B[\dA-Z]{9}"
DOWNLOAD_PATH = "arashnic/book-recommendation-dataset"