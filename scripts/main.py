import argparse
import logging
from logging.handlers import RotatingFileHandler
import os
import shutil
from datetime import datetime
import json
import requests
import csv
from pathlib import Path
from box import Box
from tenacity import (
    RetryError,
    before_sleep_log,
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)


def setup_logging(log_file_path, MAX_LOG_SIZE=10 * 1024 * 1024, BACKUP_COUNT=5):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    file_handler = RotatingFileHandler(
        log_file_path, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT
    )
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def parse_arguments(csv_path):
    script_dir = os.path.dirname(__file__)
    config_file = Path(script_dir) / "config.json"
    if config_file.is_file():
        config_file_path = os.path.join(script_dir, "config.json")
        try:
            with open(config_file_path, "r") as file:
                data = json.load(file)
                if csv_path:
                    data["csvPath"] = csv_path
                return Box(data)
        except FileNotFoundError:
            logging.error("Config file not found.")
            raise
        except json.JSONDecodeError:
            logging.error("Error decoding JSON from the config file.")
            raise

    parser = argparse.ArgumentParser(
        description="Process and upload CSV file in chunks."
    )
    parser.add_argument("--header", type=int, default=0, help="Set header line number")
    parser.add_argument(
        "--csvPath",
        type=str,
        required=True,
        default=csv_path,
        help="Path to the CSV file",
    )
    parser.add_argument(
        "--baseUrl", type=str, required=True, help="Base URL to upload chunks"
    )
    parser.add_argument(
        "--username", type=str, required=True, help="Username for authentication"
    )
    parser.add_argument(
        "--password", type=str, required=True, help="Password for authentication"
    )
    parser.add_argument(
        "--OAuthClientLevel", type=str, required=False, help="OAuth client level for authentication.Possible values: USER,SUBSCRIPTION"
    )
    parser.add_argument(
        "--clientId", type=str, required=False, help="Client_id for authentication"
    )
    parser.add_argument(
        "--clientSecret", type=str, required=False, help="Client_secret for authentication"
    )
    parser.add_argument(
        "--connectionUuid", type=str, required=True, help="Connection UUID"
    )
    parser.add_argument("--profileUuid", type=str, required=True, help="Profile UUID")
    parser.add_argument(
        "--envQualysUsernameProperty",
        type=str,
        required=False,
        help="Environment variable name for username (optional)",
    )
    parser.add_argument(
        "--envQualysPasswordProperty",
        type=str,
        required=False,
        help="Environment variable name for password (optional)",
    )
    parser.add_argument(
        "--envQualysclientIdProperty",
        type=str,
        required=False,
        help="Environment variable name for client id (optional)",
    )
    parser.add_argument(
        "--envQualysClientSecretProperty",
        type=str,
        required=False,
        help="Environment variable name for client secret (optional)",
   )

    return parser.parse_args()


class CsvUploader:
    def __init__(self, args):
        self.CSV_PATH = getattr(args, "csvPath", None)
        self.HEADER = getattr(args, "header", 1)
        self.MAX_CHUNK_SIZE = 9 * 1024 * 1024  # 9Mb
        self.BASE_URL = args.baseUrl
        self.USERNAME = getattr(args, "username", None)
        self.PASSWORD = getattr(args, "password", None)
        self.O_AUTH_CLIENT_LEVEL = getattr(args, "OAuthClientLevel", None)
        self.CLIENT_ID = getattr(args, "clientId", None)
        self.CLIENT_SECRET = getattr(args, "clientSecret", None)
        self.CONNECTION_UUID = args.connectionUuid
        self.PROFILE_UUID = args.profileUuid
        self.QAS_JWT_TOKEN_URI = "/auth"
        self.QAS_USER_LEVEL_OAUTH_JWT_TOKEN_URI = "/auth/oidc"
        self.QAS_SUBSCRIPTION_LEVEL_OAUTH_JWT_TOKEN_URI = "/auth/oauth"
        self.PROCESSED_DIR = "uploaded"
        self.envQualysUsernameProperty = getattr(
            args, "envQualysUsernameProperty", None
        )
        self.envQualysPasswordProperty = getattr(
            args, "envQualysPasswordProperty", None
        )
        self.envQualysclientIdProperty = getattr(
            args, "envQualysclientIdProperty", None
        )
        self.envQualysClientSecretProperty = getattr(
            args, "envQualysClientSecretProperty", None
        )
        if not self.O_AUTH_CLIENT_LEVEL:
            self._fill_username_password()
            self.generated_jwt = self._generate_jwt()

        else:
            self._fill_client_id_client_secret()
            self._validate_client_credentials()
            self.generated_jwt = self._generate_jwt_with_client_id_and_client_secret()

    def _fill_username_password(self):
        if (
            self.envQualysUsernameProperty is not None
            and self.envQualysPasswordProperty is not None
        ):
            logging.info("Reading username password from env variables")
            self.USERNAME = os.getenv(self.envQualysUsernameProperty)
            self.PASSWORD = os.getenv(self.envQualysPasswordProperty)
            if self.USERNAME is None or self.PASSWORD is None:
                raise RuntimeError(
                    "Env properties were provided but not set for username password"
                )
    def _fill_client_id_client_secret(self):
        if (
                self.envQualysclientIdProperty is not None
                and self.envQualysClientSecretProperty is not None
        ):
            logging.info("Reading client id and client secret from env variables")
            self.CLIENT_ID = os.getenv(self.envQualysclientIdProperty)
            self.CLIENT_SECRET = os.getenv(self.envQualysClientSecretProperty)
            if self.CLIENT_ID is None or self.CLIENT_SECRET is None:
                raise RuntimeError(
                    "Env properties were provided but not set for Client id and Client secret"
                )

    def _validate_client_credentials(self):
        if not self.CLIENT_ID:
            logging.error("Missing client_id.")
            raise ValueError("client_id must be provided and cannot be empty when OAuthClientLevel is used.")

        if not self.CLIENT_SECRET:
            logging.error("Missing client_secret.")
            raise ValueError("client_secret must be provided and cannot be empty when OAuthClientLevel is used.")

    def _generate_jwt(self):
        url = f"{self.BASE_URL}{self.QAS_JWT_TOKEN_URI}"
        payload = {
            "username": self.USERNAME,
            "password": self.PASSWORD,
            "token": "true",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        try:
            response = requests.post(url, headers=headers, data=payload, verify=False)
            response.raise_for_status()
            logging.info("JWT Generated Successfully !!")
            return response.text
        except requests.RequestException as e:
            logging.error(f"JWT generation failed: {str(e)}")
            raise

    def _generate_jwt_with_client_id_and_client_secret(self):
        valid_types = {"USER", "SUBSCRIPTION"}
        if self.O_AUTH_CLIENT_LEVEL not in valid_types:
            logging.error(f"Invalid O_AUTH_CLIENT_LEVEL: '{self.O_AUTH_CLIENT_LEVEL}'. Expected one of {valid_types}.")
            raise ValueError(f"O_AUTH_CLIENT_LEVEL must be one of {valid_types}, but got '{self.O_AUTH_CLIENT_LEVEL}'.")

        if self.O_AUTH_CLIENT_LEVEL == "USER":
            url = f"{self.BASE_URL}{self.QAS_USER_LEVEL_OAUTH_JWT_TOKEN_URI}"
        elif self.O_AUTH_CLIENT_LEVEL == "SUBSCRIPTION":
            url = f"{self.BASE_URL}{self.QAS_SUBSCRIPTION_LEVEL_OAUTH_JWT_TOKEN_URI}"

        headers = {
            "Content-Type":"application/x-www-form-urlencoded",
            "clientId": self.CLIENT_ID,
            "clientSecret": self.CLIENT_SECRET,
            "encrypted": "rsa512",
        }

        try:
            response = requests.post(url, headers=headers, verify=False)
            response.raise_for_status()
            logging.info("JWT Generated Successfully !!")
            return response.text
        except requests.RequestException as e:
            logging.error(f"JWT generation failed: {str(e)}")
            raise

    def upload_csv_file(self, chunk_filename, output_dir):
        processed_chunk_path = os.path.join(
            output_dir, self.PROCESSED_DIR, chunk_filename
        )
        chunk_file_path = os.path.join(output_dir, chunk_filename)
        try:
            self.upload_call(chunk_file_path, chunk_filename)
            # move files to uploaded file to know which files are processed
            shutil.move(chunk_file_path, processed_chunk_path)
            logging.info(f"Moved {chunk_filename} to {processed_chunk_path}.")
        except RetryError:
            logging.error(
                f"Failed to upload {chunk_file_path} after multiple attempts "
            )

    @retry(
        wait=wait_exponential(
            multiplier=10,
        ),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logging, logging.INFO),  # type: ignore
    )
    def upload_call(self, chunk_file_path, chunk_filename):
        with open(chunk_file_path, "rb") as f:
            files = {"file": (chunk_filename, f, "text/csv")}
            headers = {"Authorization": f"Bearer {self.generated_jwt}"}
            try:
                response = requests.post(
                    f"{self.BASE_URL}/connector-config/connector/integration/{self.CONNECTION_UUID}/{self.PROFILE_UUID}/file-upload",
                    files=files,
                    headers=headers,
                    verify=False,
                )
            except requests.RequestException as e:
                logging.error(f"Error uploading file {chunk_filename}: {str(e)}")
                raise

        if response.status_code == 200:
            logging.info(f"File {chunk_file_path} uploaded successfully!")
            return response
        elif response.status_code == 401:
            if not self.O_AUTH_CLIENT_LEVEL:
                self.generated_jwt = self._generate_jwt()
            else:
                self.generated_jwt = self._generate_jwt_with_client_id_and_client_secret()
            raise Exception(
                f"Upload failed with status code {response.status_code}, regenerating JWT token"
            )
        else:
            logging.warning(f"Upload failed for {chunk_file_path}: - {response.text}")
            raise Exception(f"Upload failed status code {response.status_code}")

    def process_and_upload_chunks(self):
        output_dir, filename = self._prepare_output_directory()
        chunk = []
        current_chunk_size = 0
        chunk_index = 0
        with open(file=self.CSV_PATH, mode="r", encoding="utf-8") as file:
            csv_file_reader = csv.reader(
                file, quotechar='"', quoting=csv.QUOTE_MINIMAL
            )  # Handle quoted CSV
            for _ in range(self.HEADER - 1):
                next(csv_file_reader)
            headers = next(csv_file_reader)
            for row in csv_file_reader:
                row_size = len(",".join(row).encode("utf-8")) + 1
                chunk.append(row)

                if current_chunk_size + row_size > self.MAX_CHUNK_SIZE:
                    chunk_index += 1
                    chunk_filename = self._write_chunk(
                        chunk, chunk_index, headers, filename, output_dir
                    )
                    self.upload_csv_file(chunk_filename, output_dir)
                    chunk = []
                    current_chunk_size = 0
                chunk.append(row)
                current_chunk_size += row_size
            if chunk:
                chunk_index += 1
                chunk_filename = self._write_chunk(
                    chunk, chunk_index, headers, filename, output_dir
                )
                self.upload_csv_file(chunk_filename, output_dir)

    def _write_chunk(self, chunk, chunk_index, headers, filename, output_dir):
        chunk_filename = f"{filename}_{chunk_index}.csv"
        chunk_file_path = os.path.join(output_dir, chunk_filename)

        with open(chunk_file_path, "w", newline="") as chunk_file:
            writer = csv.writer(chunk_file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(headers)
            writer.writerows(chunk)
        logging.info(
            f"Written chunk {chunk_filename} with {len(chunk)} lines to {chunk_file_path}."
        )
        return chunk_filename

    def _prepare_output_directory(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.splitext(os.path.basename(self.CSV_PATH))[0]
        directory = os.path.dirname(self.CSV_PATH)
        output_dir = os.path.join(directory, f"{filename}_{timestamp}")
        os.makedirs(os.path.join(output_dir, self.PROCESSED_DIR), exist_ok=True)
        return output_dir, filename


def start(csv_path):
    setup_logging("./script.log")
    args = parse_arguments(csv_path)
    uploader = CsvUploader(args)
    uploader.process_and_upload_chunks()


def main():
    start(None)


if __name__ == "__main__":
    main()