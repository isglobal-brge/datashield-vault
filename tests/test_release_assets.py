import re
from pathlib import Path
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
RELEASE_VERSION = "0.3.11"


class ReleaseAssetTests(unittest.TestCase):
    def test_default_compose_uses_digest_pinned_images(self):
        compose = yaml.safe_load(
            (ROOT / "docker-compose.yml").read_text(encoding="utf-8"))

        for service in ("minio", "controller"):
            image = compose["services"][service]["image"]
            with self.subTest(service=service):
                self.assertNotIn(":latest", image)
                self.assertRegex(image, r":[^@}]+@sha256:[0-9a-f]{64}\}?$")
        self.assertNotIn("build", compose["services"]["controller"])

    def test_admin_dependency_uses_full_git_revision(self):
        requirements = (ROOT / "controller" / "requirements.txt").read_text(
            encoding="utf-8")
        match = re.search(
            r"dsimaging-admin @ https://github\.com/isglobal-brge/"
            r"dsimaging-admin/archive/([0-9a-f]{40})\.tar\.gz",
            requirements,
        )

        self.assertIsNotNone(match)

    def test_controller_runtime_dependencies_are_exactly_pinned(self):
        requirements = (ROOT / "controller" / "requirements.txt").read_text(
            encoding="utf-8")
        runtime_lines = [
            line.strip() for line in requirements.splitlines()
            if line.strip() and not line.startswith("dsimaging-admin")
        ]

        self.assertTrue(runtime_lines)
        self.assertTrue(all("==" in line for line in runtime_lines))

    def test_controller_image_is_pinned_and_runs_unprivileged(self):
        dockerfile = (ROOT / "controller" / "Dockerfile").read_text(
            encoding="utf-8")

        self.assertRegex(
            dockerfile.splitlines()[0],
            r"^FROM python:3\.11-slim@sha256:[0-9a-f]{64}$",
        )
        self.assertIn("USER controller:controller", dockerfile)

        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn(
            f'org.opencontainers.image.version="{RELEASE_VERSION}"',
            dockerfile,
        )
        self.assertIn(
            f"davidsarrat/dsimaging-store:{RELEASE_VERSION}@sha256:",
            compose,
        )
        self.assertIn(
            f"docker pull davidsarrat/dsimaging-store:{RELEASE_VERSION}",
            readme,
        )

    def test_example_env_keeps_digest_pinned_controller_default(self):
        env_lines = (ROOT / ".env.example").read_text(
            encoding="utf-8").splitlines()
        active = [
            line.strip() for line in env_lines
            if line.strip() and not line.lstrip().startswith("#")
        ]

        self.assertFalse(any(
            line.startswith("DSIMAGING_STORE_CONTROLLER_IMAGE=")
            for line in active
        ))


if __name__ == "__main__":
    unittest.main()
