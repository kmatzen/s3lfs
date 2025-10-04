#!/usr/bin/env python
"""Test YAML manifest support and migration functionality."""

import json
import tempfile
import unittest
from pathlib import Path

import yaml

from s3lfs.core import S3LFS


class TestYAMLMigration(unittest.TestCase):
    """Test cases for YAML manifest support and migration."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.test_path = Path(self.test_dir)

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_yaml_manifest_creation(self):
        """Test that new repos create YAML manifests."""
        yaml_manifest = self.test_path / ".s3_manifest.yaml"

        # Create a new S3LFS instance with YAML manifest
        s3lfs = S3LFS(
            bucket_name="test-bucket",
            manifest_file=str(yaml_manifest),
            repo_prefix="test-prefix",
        )
        s3lfs.initialize_repo()

        # Verify YAML manifest was created
        self.assertTrue(yaml_manifest.exists())

        # Verify content is valid YAML
        with open(yaml_manifest, "r") as f:
            data = yaml.safe_load(f)
            self.assertIn("bucket_name", data)
            self.assertEqual(data["bucket_name"], "test-bucket")
            self.assertEqual(data["repo_prefix"], "test-prefix")
            self.assertIn("files", data)

    def test_json_backward_compatibility(self):
        """Test that existing JSON manifests still work."""
        json_manifest = self.test_path / ".s3_manifest.json"

        # Create a JSON manifest
        manifest_data = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {"test_file.txt": "abc123hash"},
        }
        with open(json_manifest, "w") as f:
            json.dump(manifest_data, f)

        # Load it with S3LFS
        s3lfs = S3LFS(
            manifest_file=str(json_manifest),
        )

        # Verify data was loaded correctly
        self.assertEqual(s3lfs.bucket_name, "test-bucket")
        self.assertEqual(s3lfs.repo_prefix, "test-prefix")
        self.assertIn("test_file.txt", s3lfs.manifest["files"])

    def test_yaml_manifest_load_and_save(self):
        """Test loading and saving YAML manifests."""
        yaml_manifest = self.test_path / ".s3_manifest.yaml"

        # Create initial YAML manifest
        manifest_data = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {
                "file1.txt": "hash1",
                "file2.txt": "hash2",
            },
        }
        with open(yaml_manifest, "w") as f:
            yaml.safe_dump(manifest_data, f)

        # Load and modify
        s3lfs = S3LFS(manifest_file=str(yaml_manifest))
        s3lfs.manifest["files"]["file3.txt"] = "hash3"
        s3lfs.save_manifest()

        # Verify changes were saved
        with open(yaml_manifest, "r") as f:
            data = yaml.safe_load(f)
            self.assertEqual(len(data["files"]), 3)
            self.assertEqual(data["files"]["file3.txt"], "hash3")

    def test_cache_file_format_matches_manifest(self):
        """Test that cache files use the same format as manifest."""
        # Test with YAML manifest
        yaml_manifest = self.test_path / ".s3_manifest.yaml"
        s3lfs = S3LFS(
            bucket_name="test-bucket",
            manifest_file=str(yaml_manifest),
            repo_prefix="test-prefix",
        )

        # Cache file should be YAML
        self.assertTrue(s3lfs.cache_file.suffix in [".yaml", ".yml"])

        # Test with JSON manifest
        json_manifest = self.test_path / ".s3_manifest.json"
        s3lfs = S3LFS(
            bucket_name="test-bucket",
            manifest_file=str(json_manifest),
            repo_prefix="test-prefix",
        )

        # Cache file should be JSON
        self.assertEqual(s3lfs.cache_file.suffix, ".json")

    def test_manual_migration(self):
        """Test manual migration from JSON to YAML."""
        json_manifest = self.test_path / ".s3_manifest.json"
        yaml_manifest = self.test_path / ".s3_manifest.yaml"

        # Create a JSON manifest
        manifest_data = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {
                "file1.txt": "hash1",
                "file2.txt": "hash2",
            },
        }
        with open(json_manifest, "w") as f:
            json.dump(manifest_data, f, indent=4, sort_keys=True)

        # Manually migrate to YAML
        with open(json_manifest, "r") as f:
            data = json.load(f)

        with open(yaml_manifest, "w") as f:
            yaml.safe_dump(data, f, default_flow_style=False, sort_keys=True)

        # Verify both work
        json_s3lfs = S3LFS(manifest_file=str(json_manifest))
        yaml_s3lfs = S3LFS(manifest_file=str(yaml_manifest))

        self.assertEqual(json_s3lfs.bucket_name, yaml_s3lfs.bucket_name)
        self.assertEqual(json_s3lfs.manifest["files"], yaml_s3lfs.manifest["files"])


if __name__ == "__main__":
    unittest.main()
