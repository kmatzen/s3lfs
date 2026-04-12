#!/usr/bin/env python
"""Test the migrate CLI command."""

import json
import os
import tempfile
import unittest
from pathlib import Path

import yaml
from click.testing import CliRunner

from s3lfs.cli import migrate


class TestMigrateCLI(unittest.TestCase):
    """Test cases for the migrate CLI command."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.test_path = Path(self.test_dir)

        # Create a fake .git directory to simulate a git repo
        (self.test_path / ".git").mkdir()

        # Save original directory
        self.original_cwd = os.getcwd()

        # Change to test directory
        os.chdir(self.test_path)

        self.runner = CliRunner()

    def tearDown(self):
        """Clean up test fixtures."""
        # Change back to original directory
        os.chdir(self.original_cwd)

        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_migrate_json_to_yaml(self):
        """Test migrating a JSON manifest to YAML."""
        json_manifest = self.test_path / ".s3_manifest.json"
        yaml_manifest = self.test_path / ".s3_manifest.yaml"

        # Create a JSON manifest
        manifest_data = {
            "bucket_name": "test-bucket",
            "repo_prefix": "test-prefix",
            "files": {
                "file1.txt": "hash1",
                "file2.txt": "hash2",
                "dir/file3.txt": "hash3",
            },
        }
        with open(json_manifest, "w") as f:
            json.dump(manifest_data, f, indent=4, sort_keys=True)

        # Run migrate command
        result = self.runner.invoke(migrate, ["--force"])

        # Check it succeeded
        self.assertEqual(result.exit_code, 0, f"Migration failed: {result.output}")
        self.assertIn("Migration complete", result.output)

        # Verify YAML manifest was created
        self.assertTrue(yaml_manifest.exists())

        # Verify content matches
        with open(yaml_manifest, "r") as f:
            yaml_data = yaml.safe_load(f)

        self.assertEqual(yaml_data["bucket_name"], manifest_data["bucket_name"])
        self.assertEqual(yaml_data["repo_prefix"], manifest_data["repo_prefix"])
        self.assertEqual(yaml_data["files"], manifest_data["files"])

        # Verify JSON manifest still exists (as backup)
        self.assertTrue(json_manifest.exists())

    def test_migrate_no_json_manifest(self):
        """Test migration fails gracefully when no JSON manifest exists."""
        result = self.runner.invoke(migrate, ["--force"])

        # Should fail with appropriate message
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("No JSON manifest found", result.output)

    def test_migrate_yaml_already_exists(self):
        """Test migration fails when YAML manifest already exists."""
        json_manifest = self.test_path / ".s3_manifest.json"
        yaml_manifest = self.test_path / ".s3_manifest.yaml"

        # Create both manifests
        with open(json_manifest, "w") as f:
            json.dump({"bucket_name": "test"}, f)

        with open(yaml_manifest, "w") as f:
            yaml.safe_dump({"bucket_name": "test"}, f)

        # Run migrate command
        result = self.runner.invoke(migrate, ["--force"])

        # Should fail
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("already exists", result.output)


if __name__ == "__main__":
    unittest.main()
