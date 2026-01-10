"""
Data loader for the movies dataset.

Handles loading, caching, and accessing movie data for the DHT implementation.
"""

import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

import config
from src.common.logger import get_logger

logger = get_logger(__name__)

# Cache for the loaded dataset
_dataset_cache: Optional[pd.DataFrame] = None


@dataclass
class MovieRecord:
    """
    Represents a single movie record from the dataset.
    
    Attributes:
        id: Unique movie identifier.
        title: Movie title (used as DHT key).
        adult: Whether the movie is adult content.
        original_language: Original language code.
        origin_country: List of production countries.
        release_date: Release date string.
        genre_names: List of genres.
        production_company_names: List of production companies.
        budget: Production budget in USD.
        revenue: Worldwide revenue in USD.
        runtime: Duration in minutes.
        popularity: TMDB popularity score.
        vote_average: Average user rating.
        vote_count: Number of votes.
    """
    id: int
    title: str
    adult: bool
    original_language: str
    origin_country: str
    release_date: str
    genre_names: str
    production_company_names: str
    budget: float
    revenue: float
    runtime: int
    popularity: float
    vote_average: float
    vote_count: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the record to a dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "adult": self.adult,
            "original_language": self.original_language,
            "origin_country": self.origin_country,
            "release_date": self.release_date,
            "genre_names": self.genre_names,
            "production_company_names": self.production_company_names,
            "budget": self.budget,
            "revenue": self.revenue,
            "runtime": self.runtime,
            "popularity": self.popularity,
            "vote_average": self.vote_average,
            "vote_count": self.vote_count,
        }
    
    @classmethod
    def from_series(cls, series: pd.Series) -> "MovieRecord":
        """Create a MovieRecord from a pandas Series (DataFrame row)."""
        return cls(
            id=int(series["id"]),
            title=str(series["title"]),
            adult=bool(series["adult"]),
            original_language=str(series["original_language"]),
            origin_country=str(series["origin_country"]),
            release_date=str(series["release_date"]),
            genre_names=str(series["genre_names"]),
            production_company_names=str(series["production_company_names"]),
            budget=float(series["budget"]),
            revenue=float(series["revenue"]),
            runtime=int(series["runtime"]),
            popularity=float(series["popularity"]),
            vote_average=float(series["vote_average"]),
            vote_count=int(series["vote_count"]),
        )


def load_dataset(force_reload: bool = False) -> pd.DataFrame:
    """
    Load the movies dataset from CSV.
    
    The dataset is cached in memory after first load.
    
    Args:
        force_reload: If True, reload from disk even if cached.
    
    Returns:
        pandas DataFrame containing the movie data.
    
    Raises:
        FileNotFoundError: If the dataset file does not exist.
    """
    global _dataset_cache
    
    if _dataset_cache is not None and not force_reload:
        logger.debug("Returning cached dataset")
        return _dataset_cache
    
    logger.info(f"Loading dataset from {config.DATASET_PATH}")
    
    df = pd.read_csv(config.DATASET_PATH, low_memory=False)
    
    # Filter out rows with null titles
    null_title_count = df[config.DATASET_KEY_COLUMN].isnull().sum()
    if null_title_count > 0:
        logger.warning(f"Filtering out {null_title_count} rows with null titles")
        df = df[df[config.DATASET_KEY_COLUMN].notnull()].copy()
    
    # Reset index after filtering
    df = df.reset_index(drop=True)
    
    logger.info(f"Dataset loaded: {len(df):,} rows, {len(df.columns)} columns")
    
    _dataset_cache = df
    return df


def get_all_titles() -> List[str]:
    """
    Get a list of all movie titles in the dataset.
    
    These are the keys we will use in the DHT.
    
    Returns:
        List of movie title strings.
    """
    df = load_dataset()
    return df[config.DATASET_KEY_COLUMN].tolist()


def get_movie_by_title(title: str) -> Optional[MovieRecord]:
    """
    Get a movie record by its exact title.
    
    Args:
        title: The exact movie title to search for.
    
    Returns:
        MovieRecord if found, None otherwise.
    """
    df = load_dataset()
    matches = df[df[config.DATASET_KEY_COLUMN] == title]
    
    if len(matches) == 0:
        return None
    
    # Return the first match (there may be duplicates with same title)
    return MovieRecord.from_series(matches.iloc[0])


def get_movie_as_dict(title: str) -> Optional[Dict[str, Any]]:
    """
    Get a movie's data as a dictionary by its exact title.
    
    Args:
        title: The exact movie title to search for.
    
    Returns:
        Dictionary of movie data if found, None otherwise.
    """
    record = get_movie_by_title(title)
    if record is None:
        return None
    return record.to_dict()


def get_sample_movies(n: int, seed: Optional[int] = None) -> List[MovieRecord]:
    """
    Get a random sample of movies from the dataset.
    
    Useful for testing with a subset of data.
    
    Args:
        n: Number of movies to sample.
        seed: Random seed for reproducibility. If None, results vary.
    
    Returns:
        List of MovieRecord objects.
    """
    df = load_dataset()
    
    if n > len(df):
        logger.warning(f"Requested {n} samples but only {len(df)} available")
        n = len(df)
    
    if seed is not None:
        random.seed(seed)
    
    indices = random.sample(range(len(df)), n)
    
    return [MovieRecord.from_series(df.iloc[i]) for i in indices]


def get_sample_titles(n: int, seed: Optional[int] = None) -> List[str]:
    """
    Get a random sample of movie titles from the dataset.
    
    Useful for testing lookups with a subset of keys.
    
    Args:
        n: Number of titles to sample.
        seed: Random seed for reproducibility. If None, results vary.
    
    Returns:
        List of movie title strings.
    """
    samples = get_sample_movies(n, seed)
    return [movie.title for movie in samples]


def get_dataset_stats() -> Dict[str, Any]:
    """
    Get statistics about the dataset.
    
    Returns:
        Dictionary containing dataset statistics.
    """
    df = load_dataset()
    
    return {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "columns": list(df.columns),
        "unique_titles": df[config.DATASET_KEY_COLUMN].nunique(),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
        "null_counts": df.isnull().sum().to_dict(),
    }


def clear_cache() -> None:
    """Clear the cached dataset to free memory."""
    global _dataset_cache
    _dataset_cache = None
    logger.info("Dataset cache cleared")
