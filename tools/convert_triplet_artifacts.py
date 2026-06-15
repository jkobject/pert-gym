#!/usr/bin/env python3
"""Minimal LaminDB converters for legacy AnnData datasets.

Use only these two functions:
- ``migrate_h5ad_to_triplet``: legacy ``.h5ad`` -> ``obs.parquet`` + ``X.h5ad`` +
  ``var.parquet`` with links ``obs -> X -> var``.
- ``migrate_triplet_to_zarr``: triplet -> ``X.zarr``.

Both functions can replace old artifacts on the instance by creating revised
records and deleting the source artifacts.
"""

from __future__ import annotations

from typing import Any

import anndata as ad
import lamindb as ln
import pandas as pd


def _get_registry(db: Any) -> Any:
    """Return a writable Lamin registry namespace for the target instance."""
    target_instance = getattr(db, "_instance", None)
    if isinstance(target_instance, str):
        current = ln.setup.settings.instance.slug
        if current != target_instance:
            ln.connect(target_instance)
    return ln


def _prefix_from_key(key: str) -> str:
    """Normalize an artifact key/prefix into the dataset prefix."""
    key = key.rstrip("/")
    return key[:-5] if key.endswith(".h5ad") else key


def migrate_h5ad_to_triplet(
    dataset_name: str | ad.AnnData,
    db: Any,
    *,
    dataset_prefix: str | None = None,
    replace_on_instance: bool = True,
    storage=True,
    skip_hash_lookup_for_var: bool = True,
) -> dict[str, Any]:
    """Migrate one legacy h5ad artifact into the canonical 3-file layout.

    Parameters
    ----------
    dataset_name
        Either an explicit key ending with ``.h5ad`` or a dataset prefix that
        resolves to one h5ad artifact. Can also be an in-memory ``AnnData``.
    db
        LaminDB module (example: ``import lamindb as ln``).
    dataset_prefix
        Dataset prefix to use when ``dataset_name`` is an ``AnnData``. Ignored
        when a string key/prefix is provided.
    replace_on_instance
        If True (default):
        - writes new latest versions for existing output keys (via ``revises``)
        - deletes the legacy source ``.h5ad`` artifact after successful write
    skip_hash_lookup_for_var
        If True (default), write a dataset-specific ``var.parquet`` artifact
        even when its dataframe hash matches another dataset. Many PRISM
        datasets share the same gene table, but the canonical triplet layout
        still expects a ``var`` artifact under each dataset prefix.

    Returns
    -------
    dict[str, Any]
        ``{"obs": obs_artifact, "X": x_artifact, "var": var_artifact}``.
    """
    r = _get_registry(db)
    source = None
    if isinstance(dataset_name, str):
        if dataset_name.endswith(".h5ad"):
            source = r.Artifact.get(key=dataset_name)
        else:
            candidates = list(r.Artifact.filter(key=dataset_name, suffix=".h5ad").all())
            candidates += list(r.Artifact.filter(key=f"{dataset_name}.h5ad").all())
            if not candidates:
                candidates = list(
                    r.Artifact.filter(
                        key__startswith=f"{dataset_name.rstrip('/')}/", suffix=".h5ad"
                    ).all()
                )
            if not candidates:
                raise ValueError(f"No .h5ad artifact found for '{dataset_name}'.")
            source = sorted(candidates, key=lambda a: a.created_at)[-1]

        source_key = source.key
        if source_key is None:
            raise ValueError("Source artifact has no key.")
        prefix = _prefix_from_key(source_key)
        adata = source.load()
    elif isinstance(dataset_name, ad.AnnData):
        adata = dataset_name
        if dataset_prefix is None:
            raise ValueError(
                "dataset_prefix is required when dataset_name is an AnnData object."
            )
        prefix = _prefix_from_key(dataset_prefix)
    else:
        raise TypeError(
            "dataset_name must be a dataset name string or an AnnData object."
        )

    obs_key = f"{prefix}/obs.parquet"
    x_key = f"{prefix}/X.h5ad"
    var_key = f"{prefix}/var.parquet"

    for name in ("X", "var"):
        feature = list(r.Feature.filter(name=name).all())
        if feature and feature[0].dtype != "cat[Artifact]":
            raise ValueError(
                f"Feature '{name}' has dtype '{feature[0].dtype}', expected 'cat[Artifact]'."
            )
        if not feature:
            r.Feature(name=name, dtype="cat[Artifact]").save()

    if not isinstance(adata, ad.AnnData):
        raise TypeError(f"Expected AnnData, got {type(adata).__name__}.")

    obs_df = adata.obs.copy()
    var_df = adata.var.copy()
    x_adata = adata.copy()
    x_adata.obs = pd.DataFrame(index=adata.obs_names)
    x_adata.var = pd.DataFrame(index=adata.var_names)

    prev_obs = list(r.Artifact.filter(key=obs_key).all())
    prev_x = list(r.Artifact.filter(key=x_key).all())
    prev_var = list(r.Artifact.filter(key=var_key).all())

    obs_artifact = r.Artifact.from_dataframe(
        obs_df,
        key=obs_key,
        revises=prev_obs[-1] if (replace_on_instance and prev_obs) else None,
    ).save()
    x_artifact = r.Artifact.from_anndata(
        x_adata,
        key=x_key,
        revises=prev_x[-1] if (replace_on_instance and prev_x) else None,
    ).save()
    var_artifact = r.Artifact.from_dataframe(
        var_df,
        key=var_key,
        revises=prev_var[-1] if (replace_on_instance and prev_var) else None,
        skip_hash_lookup=skip_hash_lookup_for_var,
    ).save()

    # Canonical links used throughout this project.
    x_artifact.features.set_values({"var": var_artifact})
    obs_artifact.features.set_values({"X": x_artifact})

    if replace_on_instance and source is not None:
        source.delete(storage=storage)

    return {"obs": obs_artifact, "X": x_artifact, "var": var_artifact}


def migrate_triplet_to_zarr(
    dataset_name: str,
    db: Any,
    *,
    replace_on_instance: bool = False,
    storage=True,
) -> Any:
    """Rebuild AnnData from triplet artifacts and save as ``X.zarr``.

    Parameters
    ----------
    dataset_name
        Dataset prefix (``.../<dataset>``) or explicit ``.../obs.parquet`` key.
    db
        LaminDB module (example: ``import lamindb as ln``).
    replace_on_instance
        If True:
        - revises existing ``X.zarr`` for this dataset if present
        - deletes source triplet artifacts (obs/X/var) after writing zarr
    """
    r = _get_registry(db)

    obs_key = (
        dataset_name
        if dataset_name.endswith("/obs.parquet")
        else f"{dataset_name}/obs.parquet"
    )
    obs_artifact = r.Artifact.get(key=obs_key)

    x_artifact = obs_artifact.features.get_values()["X"]
    var_artifact = x_artifact.features.get_values()["var"]

    adata = x_artifact.load()
    adata.obs = obs_artifact.load()
    adata.var = var_artifact.load()

    dataset_prefix = obs_key[: -len("/obs.parquet")]
    zarr_key = f"{dataset_prefix}/X.zarr"
    prev_zarr = list(r.Artifact.filter(key=zarr_key).all())

    zarr_artifact = r.Artifact.from_anndata(
        adata,
        key=zarr_key,
        revises=prev_zarr[-1] if (replace_on_instance and prev_zarr) else None,
    ).save()

    if replace_on_instance:
        obs_artifact.delete(storage=storage)
        x_artifact.delete(storage=storage)
        var_artifact.delete(storage=storage)

    return zarr_artifact
