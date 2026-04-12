import numpy as np
import pandas as pd
from prefect import task
from src.utils.logger import get_logger
from src.config.constants import CURRENT_YEAR, ISBN_REGEX, ASIN_REGEX
from src.config.paths import RAW_DIR, PROCESSED_DIR, QUARANTINE_DIR
from prefect.cache_policies import NO_CACHE

logger = get_logger(__name__)

SCHEMA: dict = {
    "books": [
        {
            "col":     "ISBN",
            "mask_fn": lambda s: s == "",
            "message": "ISBN is empty",
        },
        {
            "col":     "ISBN",
            "mask_fn": lambda s: s.str.fullmatch(ASIN_REGEX, na=False),
            "message": lambda s: "ASIN not ISBN: " + s,
        },
        {
            "col":     "ISBN",
            "mask_fn": lambda s: ~s.str.fullmatch(ISBN_REGEX, na=False) & (s != ""),
            "message": lambda s: "Malformed ISBN: " + s,
        },
        {
            "col":     "Book-Title",
            "mask_fn": lambda s: s == "",
            "message": "Book-Title is empty",
        },
        {
            "col":     "Book-Title",
            "mask_fn": lambda s: s.str.fullmatch(ISBN_REGEX, na=False),
            "message": lambda s: "Book-Title looks like an ISBN: " + s,
        },
        {
            "col":     "Book-Author",
            "mask_fn": lambda s: s == "",
            "message": "Book-Author is empty",
        },
        {
            "col":     "Book-Author",
            "mask_fn": lambda s: s.str.len() <= 1,
            "message": lambda s: "Book-Author too short: " + s,
        },
        {
            "col":     "Book-Author",
            "mask_fn": lambda s: s.str.fullmatch(r"\d{4}", na=False),
            "message": lambda s: "Book-Author looks like a year: " + s,
        },
        {
            "col":     "Year-Of-Publication",
            "mask_fn": lambda s: s.notna() & ~s.between(1450, CURRENT_YEAR),
            "message": lambda s: "Year out of range: " + s.astype(str),
        },
    ],
    "users": [
        {
            "col":     "Age",
            "mask_fn": lambda s: s.notna() & ~s.between(0, 120),
            "message": lambda s: "Age out of range: " + s.astype(str),
        },
    ],
    "ratings": [
        {
            "col":     "Book-Rating",
            "mask_fn": lambda s: s.isna(),
            "message": "Rating is null",
        },
        {
            "col":     "Book-Rating",
            "mask_fn": lambda s: s.notna() & ~s.between(0, 10),
            "message": lambda s: "Rating out of range: " + s.astype(str),
        },
    ],
}


# 1. LOAD

def _load_raw(path) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        encoding="latin-1",
        dtype=str,
        keep_default_na=False,
        low_memory=False,
    )
    df.columns = df.columns.str.strip()
    return df


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("Loading raw data...")
    books   = _load_raw(RAW_DIR / "Books.csv")
    users   = _load_raw(RAW_DIR / "Users.csv")
    ratings = _load_raw(RAW_DIR / "Ratings.csv")
    logger.info(
        f"Loaded — books: {len(books):,}  "
        f"users: {len(users):,}  "
        f"ratings: {len(ratings):,}"
    )
    return books, users, ratings


# 2. STANDARDIZE

def _standardize_books(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes("object").columns:
        df[col] = df[col].str.strip()
    df["Book-Author"] = df["Book-Author"].str.title()
    df["Publisher"]   = df["Publisher"].str.title()
    df["ISBN"]        = df["ISBN"].str.upper()
    df["Year-Of-Publication"] = (
        pd.to_numeric(df["Year-Of-Publication"], errors="coerce").astype("Int64")
    )
    return df


def _standardize_users(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Location"] = df["Location"].str.strip().str.lower()
    df["User-ID"]  = pd.to_numeric(df["User-ID"], errors="coerce").astype("Int64")
    df["Age"]      = pd.to_numeric(df["Age"],     errors="coerce").astype("Int64")
    return df


def _standardize_ratings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ISBN"]        = df["ISBN"].str.strip().str.upper()
    df["User-ID"]     = pd.to_numeric(df["User-ID"],     errors="coerce").astype("Int64")
    df["Book-Rating"] = pd.to_numeric(df["Book-Rating"], errors="coerce").astype("Int64")
    return df


def standardize(
    books: pd.DataFrame,
    users: pd.DataFrame,
    ratings: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("Standardizing columns...")
    return (
        _standardize_books(books),
        _standardize_users(users),
        _standardize_ratings(ratings),
    )


# 3. VALIDATE

def _apply_schema(df: pd.DataFrame, rules: list) -> pd.Series:
    reason_parts = []
    for rule in rules:
        col     = rule["col"]
        mask_fn = rule["mask_fn"]
        message = rule["message"]
        if col not in df.columns:
            continue
        bad_mask = mask_fn(df[col])
        if callable(message):
            reasons = pd.Series("", index=df.index)
            reasons[bad_mask] = message(df.loc[bad_mask, col])
        else:
            reasons = pd.Series(np.where(bad_mask, message, ""), index=df.index)
        reason_parts.append(reasons)
    if not reason_parts:
        return pd.Series("", index=df.index)
    reason_df = pd.concat(reason_parts, axis=1)
    reason_df.columns = range(len(reason_df.columns))
    return (
        reason_df.stack()
                 .loc[lambda s: s != ""]
                 .groupby(level=0)
                 .agg(" | ".join)
                 .reindex(df.index, fill_value="")
    )


def _validate_and_split(
    df: pd.DataFrame, rules: list
) -> tuple[pd.DataFrame, pd.DataFrame]:
    combined = _apply_schema(df, rules)
    bad_mask = combined != ""
    good_df  = df[~bad_mask].reset_index(drop=True)
    bad_df   = df[bad_mask].copy()
    bad_df["reason"] = combined[bad_mask].values
    return good_df, bad_df.reset_index(drop=True)


# 4. REFERENTIAL INTEGRITY

def _check_referential_integrity(
    ratings_clean: pd.DataFrame,
    books_clean:   pd.DataFrame,
    users_clean:   pd.DataFrame,
    ratings_bad:   pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    orphan_isbn_mask = ~ratings_clean["ISBN"].isin(books_clean["ISBN"])
    orphan_user_mask = ~ratings_clean["User-ID"].isin(users_clean["User-ID"])
    orphan_isbn           = ratings_clean[orphan_isbn_mask].copy()
    orphan_isbn["reason"] = "ISBN not found in books"
    orphan_user           = ratings_clean[orphan_user_mask & ~orphan_isbn_mask].copy()
    orphan_user["reason"] = "User-ID not found in users"
    logger.info(
        f"Referential integrity — "
        f"orphan ISBNs: {len(orphan_isbn):,}  "
        f"orphan users: {len(orphan_user):,}"
    )
    return (
        ratings_clean[~orphan_isbn_mask & ~orphan_user_mask].reset_index(drop=True),
        pd.concat([ratings_bad, orphan_isbn, orphan_user], ignore_index=True),
    )


# 5. DUPLICATES

def _drop_duplicates(
    df: pd.DataFrame,
    subset: list,
    bad_df: pd.DataFrame,
    reason: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    mask            = df.duplicated(subset=subset, keep="first")
    dupes           = df[mask].copy()
    dupes["reason"] = reason
    logger.info(f"Duplicates {subset}: {mask.sum():,} quarantined")
    return (
        df[~mask].reset_index(drop=True),
        pd.concat([bad_df, dupes], ignore_index=True),
    )


# 6. EXPORT

def _export(
    books_clean:   pd.DataFrame,
    users_clean:   pd.DataFrame,
    ratings_clean: pd.DataFrame,
    books_bad:     pd.DataFrame,
    users_bad:     pd.DataFrame,
    ratings_bad:   pd.DataFrame,
) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    ts = pd.Timestamp.now()
    books_bad["quarantined_at"]   = ts
    users_bad["quarantined_at"]   = ts
    ratings_bad["quarantined_at"] = ts

    books_clean.to_csv(   PROCESSED_DIR  / "books_good.csv",        index=False)
    users_clean.to_csv(   PROCESSED_DIR  / "users_good.csv",        index=False)
    ratings_clean.to_csv( PROCESSED_DIR  / "ratings_good.csv",      index=False)
    books_bad.to_csv(     QUARANTINE_DIR / "books_quarantine.csv",   index=False)
    users_bad.to_csv(     QUARANTINE_DIR / "users_quarantine.csv",   index=False)
    ratings_bad.to_csv(   QUARANTINE_DIR / "ratings_quarantine.csv", index=False)
    logger.info("Exported processed and quarantine files.")


# 7. SUMMARY

def _log_summary(
    books_clean:   pd.DataFrame,
    users_clean:   pd.DataFrame,
    ratings_clean: pd.DataFrame,
    books_bad:     pd.DataFrame,
    users_bad:     pd.DataFrame,
    ratings_bad:   pd.DataFrame,
) -> None:
    for name, clean, bad in [
        ("Books",   books_clean,   books_bad),
        ("Users",   users_clean,   users_bad),
        ("Ratings", ratings_clean, ratings_bad),
    ]:
        total = len(clean) + len(bad)
        logger.info(
            f"{name}: total={total:,}  "
            f"clean={len(clean):,} ({len(clean)/total*100:.1f}%)  "
            f"quarantined={len(bad):,} ({len(bad)/total*100:.1f}%)"
        )
        for reason, count in bad["reason"].value_counts().head(5).items():
            logger.info(f"  {count:>6,}x  {reason}")


# 8. RUN

@task(name="validate-data", cache_policy=NO_CACHE)
def validate(export_results: bool = True):
    logger.info("=== VALIDATION PIPELINE START ===")

    books, users, ratings = load_data()
    books, users, ratings = standardize(books, users, ratings)

    logger.info("Running schema validation...")
    books_c,   books_b   = _validate_and_split(books,   SCHEMA["books"])
    users_c,   users_b   = _validate_and_split(users,   SCHEMA["users"])
    ratings_c, ratings_b = _validate_and_split(ratings, SCHEMA["ratings"])

    ratings_c, ratings_b = _check_referential_integrity(
        ratings_c, books_c, users_c, ratings_b
    )

    books_c,   books_b   = _drop_duplicates(books_c,   ["ISBN"],            books_b,   "Duplicate ISBN")
    users_c,   users_b   = _drop_duplicates(users_c,   ["User-ID"],         users_b,   "Duplicate User-ID")
    ratings_c, ratings_b = _drop_duplicates(ratings_c, ["User-ID", "ISBN"], ratings_b, "Duplicate (User-ID, ISBN)")

    if export_results:
        _export(books_c, users_c, ratings_c, books_b, users_b, ratings_b)

    _log_summary(books_c, users_c, ratings_c, books_b, users_b, ratings_b)

    logger.info("=== VALIDATION PIPELINE DONE ===")

    return books_c, users_c, ratings_c, books_b, users_b, ratings_b