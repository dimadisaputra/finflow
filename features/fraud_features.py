"""Fraud feature extraction — future scope.

This module will contain feature engineering logic for ML-based
fraud detection models.  Currently a placeholder.

Planned features:
- Transaction velocity (rolling count per user per time window)
- Amount deviation (z-score from user's historical average)
- Geographic anomaly score (distance between consecutive transactions)
- Time-of-day patterns (unusual transaction times)
- Cross-source correlation (same user across multiple sources)
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def extract_features() -> None:
    """Extract fraud detection features from Silver data.

    TODO: Implement feature engineering pipeline.
    """
    raise NotImplementedError("Feature extraction is not yet implemented")
