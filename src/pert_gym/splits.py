"""Train / val / test split by perturbation identity."""

from __future__ import annotations

import random
from typing import Sequence


def split_perturbations(
    perturbations: Sequence[str],
    val_frac: float = 0.1,
    test_frac: float = 0.2,
    control_label: str = "control",
    seed: int = 42,
) -> tuple[list[str], list[str], list[str]]:
    """Split unique perturbation labels into train / val / test sets.

    Control cells are always included in the training split (by label).
    The returned lists contain perturbation *labels*, not cell indices.

    Args:
        perturbations: All perturbation labels present in the dataset
            (may contain duplicates — unique labels are extracted internally).
        val_frac: Fraction of non-control perturbations assigned to val.
        test_frac: Fraction of non-control perturbations assigned to test.
        control_label: Label used for control / unperturbed cells.
        seed: Random seed for reproducibility.

    Returns:
        (train_labels, val_labels, test_labels) — each a list of perturbation
        strings.  Control appears only in *train_labels*.
    """
    unique = sorted({p for p in perturbations if p != control_label})
    rng = random.Random(seed)
    rng.shuffle(unique)

    n = len(unique)
    n_test = max(1, round(n * test_frac))
    n_val = max(1, round(n * val_frac))

    test_labels = unique[:n_test]
    val_labels = unique[n_test : n_test + n_val]
    train_labels = unique[n_test + n_val :]

    return [control_label] + train_labels, val_labels, test_labels


def cell_indices_for_split(
    perturbations: Sequence[str],
    train_labels: Sequence[str],
    val_labels: Sequence[str],
    test_labels: Sequence[str],
) -> tuple[list[int], list[int], list[int]]:
    """Return cell (row) indices belonging to each split.

    Args:
        perturbations: Per-cell perturbation label array.
        train_labels, val_labels, test_labels: Output of :func:`split_perturbations`.

    Returns:
        (train_idx, val_idx, test_idx) — lists of integer indices into
        ``perturbations``.
    """
    train_set = set(train_labels)
    val_set = set(val_labels)
    test_set = set(test_labels)

    train_idx, val_idx, test_idx = [], [], []
    for i, p in enumerate(perturbations):
        if p in train_set:
            train_idx.append(i)
        elif p in val_set:
            val_idx.append(i)
        elif p in test_set:
            test_idx.append(i)

    return train_idx, val_idx, test_idx
