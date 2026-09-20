import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import sys


sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))

from main import CsvUploader


class TlsVerificationTests(unittest.TestCase):
    def setUp(self):
        self.uploader = object.__new__(CsvUploader)
        self.uploader.BASE_URL = "https://example.test"
        self.uploader.USERNAME = "user"
        self.uploader.PASSWORD = "password"
        self.uploader.O_AUTH_CLIENT_LEVEL = None
        self.uploader.CONNECTION_UUID = "connection"
        self.uploader.PROFILE_UUID = "profile"
        self.uploader.generated_jwt = "token"
        self.uploader.QAS_JWT_TOKEN_URI = "/auth"
        self.uploader.QAS_USER_LEVEL_OAUTH_JWT_TOKEN_URI = "/auth/oidc"

    @patch("main.requests.post")
    def test_password_auth_uses_requests_default_certificate_verification(self, post):
        post.return_value = Mock(text="jwt")

        self.uploader._generate_jwt()

        self.assertNotIn("verify", post.call_args.kwargs)

    @patch("main.requests.post")
    def test_upload_uses_requests_default_certificate_verification(self, post):
        post.return_value = Mock(status_code=200)

        with tempfile.NamedTemporaryFile() as chunk:
            self.uploader.upload_call(chunk.name, "chunk.csv")

        self.assertNotIn("verify", post.call_args.kwargs)

    @patch("main.requests.post")
    def test_oauth_auth_uses_requests_default_certificate_verification(self, post):
        post.return_value = Mock(text="jwt")
        self.uploader.O_AUTH_CLIENT_LEVEL = "USER"
        self.uploader.CLIENT_ID = "client"
        self.uploader.CLIENT_SECRET = "secret"

        self.uploader._generate_jwt_with_client_id_and_client_secret()

        self.assertNotIn("verify", post.call_args.kwargs)


if __name__ == "__main__":
    unittest.main()
