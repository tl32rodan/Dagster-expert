"""UI smoke via GraphQL — verify everything the UI calls works end-to-end.

This exercises the same endpoints the React UI calls when you click
through Asset Catalog, Asset Graph, Partitions, and Materialize. Lives
under tests/integration/ but only runs when ``DAGSTER_GRAPHQL_URL`` is
set (default skipped). Run manually after ``dagster dev`` is up:

    DAGSTER_GRAPHQL_URL=http://127.0.0.1:3000/graphql \\
        python3 -m pytest tests/integration/test_ui_graphql.py -v
"""
from __future__ import annotations

import json
import os
import urllib.request

import pytest

_URL = os.environ.get("DAGSTER_GRAPHQL_URL")


def _server_reachable() -> bool:
    if not _URL:
        return False
    try:
        urllib.request.urlopen(_URL.replace("/graphql", "/server_info"), timeout=2)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _server_reachable(),
    reason="DAGSTER_GRAPHQL_URL unset or server not reachable. "
    "Run `dagster dev -w workspace.yaml` first.",
)


def gql(query: str, variables: dict | None = None) -> dict:
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        _URL,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = json.loads(resp.read())
    if "errors" in body:
        raise AssertionError(f"GraphQL errors: {body['errors']}")
    return body["data"]


# ── what the UI's Asset Catalog page does ─────────────────────────


def test_asset_catalog_lists_all_assets():
    """Equivalent to the 'Assets' tab loading."""
    data = gql("""
        query { assetsOrError { __typename
            ... on AssetConnection {
                nodes { id key { path } }
            }
        } }
    """)
    assert data["assetsOrError"]["__typename"] == "AssetConnection"
    paths = {tuple(n["key"]["path"]) for n in data["assetsOrError"]["nodes"]}
    # 21 lib_a/* steps + 2 source observables
    assert len(paths) == 23
    assert ("lib_a", "step5") in paths
    assert ("pvt_manifest",) in paths


# ── what the UI's Asset Graph page does ───────────────────────────


def test_asset_graph_shows_dependencies():
    """Equivalent to opening the lineage view on lib_a/step5."""
    data = gql("""
        query {
          assetNodeOrError(assetKey: {path: ["lib_a", "step5"]}) {
            __typename
            ... on AssetNode {
              id
              assetKey { path }
              dependencyKeys { path }
              dependedByKeys { path }
              partitionDefinition { name description }
            }
          }
        }
    """)
    node = data["assetNodeOrError"]
    assert node["__typename"] == "AssetNode"
    deps = {tuple(d["path"]) for d in node["dependencyKeys"]}
    assert ("lib_a", "step4") in deps
    assert ("lib_a", "step0") in deps


# ── what the UI's Partitions tab does ─────────────────────────────


def test_partitions_tab_shows_46_branches_on_step5():
    data = gql("""
        query {
          assetNodeOrError(assetKey: {path: ["lib_a", "step5"]}) {
            ... on AssetNode {
              partitionKeys
            }
          }
        }
    """)
    keys = data["assetNodeOrError"]["partitionKeys"]
    assert len(keys) == 46
    assert "corner" in keys
    assert "tmsf_lde23" in keys


def test_partitions_tab_shows_1_partition_on_step0():
    data = gql("""
        query {
          assetNodeOrError(assetKey: {path: ["lib_a", "step0"]}) {
            ... on AssetNode {
              partitionKeys
            }
          }
        }
    """)
    keys = data["assetNodeOrError"]["partitionKeys"]
    assert keys == ["corner"]


# ── partition-mapping resolution as the UI shows on hover ─────────


def test_step5_node_has_partition_definition_and_compute_kind():
    """When the UI hovers step5 it shows the partition def + compute kind."""
    data = gql("""
        query {
          assetNodeOrError(assetKey: {path: ["lib_a", "step5"]}) {
            ... on AssetNode {
              computeKind
              groupName
              partitionDefinition { name description }
              isPartitioned
            }
          }
        }
    """)
    node = data["assetNodeOrError"]
    assert node["computeKind"] == "python_pipes"
    assert node["isPartitioned"] is True
    assert node["groupName"] == "lib_a__char"


# ── source-asset observation (change-event propagation) ───────────


def test_source_pvt_manifest_node_present():
    """The PVT manifest source asset is observable but not materializable."""
    data = gql("""
        query {
          assetNodeOrError(assetKey: {path: ["pvt_manifest"]}) {
            __typename
            ... on AssetNode {
              isObservable
              isMaterializable
              groupName
            }
          }
        }
    """)
    node = data["assetNodeOrError"]
    assert node["__typename"] == "AssetNode"
    assert node["isObservable"] is True
    assert node["isMaterializable"] is False
    assert node["groupName"] == "sources"


# ── group_name layout the UI uses to bucket assets ────────────────


def test_step6_groups_as_char_runner_perl():
    data = gql("""
        query {
          assetNodeOrError(assetKey: {path: ["lib_a", "step6"]}) {
            ... on AssetNode {
              groupName
              computeKind
            }
          }
        }
    """)
    n = data["assetNodeOrError"]
    assert n["groupName"] == "lib_a__char"
    assert n["computeKind"] == "perl"


def test_kit_meta_groups_as_kit_root_only():
    data = gql("""
        query {
          assetNodeOrError(assetKey: {path: ["lib_a", "meta"]}) {
            ... on AssetNode {
              groupName
              dependencyKeys { path }
            }
          }
        }
    """)
    n = data["assetNodeOrError"]
    assert n["groupName"] == "lib_a__kit_root_only"
    deps = {tuple(d["path"]) for d in n["dependencyKeys"]}
    assert ("lib_a", "step0") in deps
    assert ("lib_a", "step6") in deps
