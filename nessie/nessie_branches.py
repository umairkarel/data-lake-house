"""
nessie_branches.py
------------------
Python utilities for Nessie Git-like branching operations on Iceberg tables.

Uses the `pynessie` client to:
  - List branches
  - Create a dev branch from main
  - Merge dev → main after validation
  - Tag a snapshot

Usage:
    python nessie/nessie_branches.py --list
    python nessie/nessie_branches.py --create dev
    python nessie/nessie_branches.py --merge dev --into main
    python nessie/nessie_branches.py --tag v1.0
"""

import argparse
import sys
from pathlib import Path
import yaml

CONFIG_PATH = Path(__file__).parent.parent / "catalog" / "catalog_config.yaml"


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def get_client(cfg):
    from pynessie import init

    nessie_uri = cfg["catalog"]["uri"]
    # pynessie init takes the base URL (without /api/v1)
    base_url = nessie_uri.replace("/api/v1", "")
    client = init(endpoint=nessie_uri)
    return client


def list_branches(client):
    refs = client.list_references().references
    print("Branches / Tags:")
    for ref in refs:
        print(f"  [{ref.type}] {ref.name}  @ {ref.hash_}")


def create_branch(client, branch_name: str, from_branch: str = "main"):
    refs = {r.name: r for r in client.list_references().references}
    source = refs.get(from_branch)
    if not source:
        print(f"[ERROR] Source branch '{from_branch}' not found.")
        return

    client.create_branch(branch_name, source.hash_)
    print(f"[OK] Branch '{branch_name}' created from '{from_branch}' @ {source.hash_}")


def merge_branch(client, from_branch: str, into_branch: str):
    refs = {r.name: r for r in client.list_references().references}
    source = refs.get(from_branch)
    target = refs.get(into_branch)

    if not source:
        print(f"[ERROR] Source branch '{from_branch}' not found.")
        return
    if not target:
        print(f"[ERROR] Target branch '{into_branch}' not found.")
        return

    client.merge(
        from_hash=source.hash_,
        onto_branch=into_branch,
        expected_hash=target.hash_,
    )
    print(f"[OK] Merged '{from_branch}' → '{into_branch}'")


def create_tag(client, tag_name: str, from_branch: str = "main"):
    refs = {r.name: r for r in client.list_references().references}
    source = refs.get(from_branch)
    if not source:
        print(f"[ERROR] Branch '{from_branch}' not found.")
        return

    client.create_tag(tag_name, source.hash_)
    print(f"[OK] Tag '{tag_name}' created at {source.hash_}")


def delete_branch(client, branch_name: str):
    refs = {r.name: r for r in client.list_references().references}
    branch = refs.get(branch_name)
    if not branch:
        print(f"[ERROR] Branch '{branch_name}' not found.")
        return

    client.delete_branch(branch_name, branch.hash_)
    print(f"[OK] Branch '{branch_name}' deleted.")


def main():
    parser = argparse.ArgumentParser(description="Nessie branch / tag operations")
    parser.add_argument("--list", action="store_true", help="List all branches and tags")
    parser.add_argument("--create", metavar="BRANCH", help="Create a new branch")
    parser.add_argument("--from", dest="from_ref", default="main", help="Source branch (default: main)")
    parser.add_argument("--merge", metavar="BRANCH", help="Branch to merge (into --into)")
    parser.add_argument("--into", metavar="TARGET", default="main", help="Merge target (default: main)")
    parser.add_argument("--tag", metavar="TAG", help="Create a tag on --from branch")
    parser.add_argument("--delete", metavar="BRANCH", help="Delete a branch")
    args = parser.parse_args()

    cfg = load_config()
    client = get_client(cfg)

    if args.list:
        list_branches(client)
    elif args.create:
        create_branch(client, args.create, args.from_ref)
    elif args.merge:
        merge_branch(client, args.merge, args.into)
    elif args.tag:
        create_tag(client, args.tag, args.from_ref)
    elif args.delete:
        delete_branch(client, args.delete)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
