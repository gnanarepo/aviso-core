import boto3
import os
import logging
import argparse

# -----------------------------
# Configuration
# -----------------------------
WHEEL_BUCKET = "aviso-core-wheel"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WheelS3Manager:
    def __init__(self, bucket=WHEEL_BUCKET):
        self.bucket = bucket
        self.s3 = boto3.client(
                "s3",
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
            )

    # ---------------------------------------------------
    # Upload wheel file to S3
    # ---------------------------------------------------
    def upload_wheel(self, local_path, key=None):
        try:
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"{local_path} does not exist")

            filename = os.path.basename(local_path)

            # Extract package name (before first '-')
            package_name = filename.split("-")[0]

            if key is None:
                key = f"{package_name}/{filename}"

            with open(local_path, "rb") as f:
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=f
                )

            logger.info(f"Uploaded {local_path} -> s3://{self.bucket}/{key}")

            return {"success": True, "s3_key": key}

        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return {"success": False, "error": str(e)}
    # ---------------------------------------------------
    # Download wheel from S3
    # ---------------------------------------------------
    def download_wheel(self, file_name, download_path=None):
        try:
            package = file_name.split("-")[0]
            key = f"{package}/{file_name}"

            if download_path is None:
                download_path = file_name

            obj = self.s3.get_object(
                Bucket=self.bucket,
                Key=key
            )

            with open(download_path, "wb") as f:
                f.write(obj["Body"].read())

            logger.info(f"Downloaded {key}")

            return {"success": True, "path": download_path}

        except Exception as e:
            logger.error(f"Download failed: {e}")
            return {"success": False, "error": str(e)}

    # ---------------------------------------------------
    # Check if wheel exists
    # ---------------------------------------------------
    def wheel_exists(self, file_name):
        try:
            # If user already passed full S3 key
            if "/" in file_name:
                key = file_name
            else:
                package = file_name.split("-")[0]
                key = f"{package}/{file_name}"

            self.s3.head_object(
                Bucket=self.bucket,
                Key=key
            )

            return True

        except Exception:
            return False
    # ---------------------------------------------------
    # List wheels in S3 bucket
    # ---------------------------------------------------
    def list_wheels(self):
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket)

            if "Contents" not in response:
                return []

            return [obj["Key"] for obj in response["Contents"]]

        except Exception as e:
            logger.error(e)
            return []

    # ---------------------------------------------------
    # Backup all wheels in folder
    # ---------------------------------------------------
    def backup_all_wheels(self, folder="wheels"):
        try:
            uploaded = []

            for file in os.listdir(folder):
                if file.endswith(".whl"):
                    path = os.path.join(folder, file)

                    result = self.upload_wheel(path)

                    if result["success"]:
                        uploaded.append(file)

            logger.info(f"Backup complete: {len(uploaded)} wheels uploaded")

            return uploaded

        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return []


# ---------------------------------------------------
# CLI Interface
# ---------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Wheel S3 Manager")

    parser.add_argument("action", choices=[
        "upload",
        "download",
        "list",
        "exists",
        "backup"
    ])

    parser.add_argument("--file", help="Wheel file name")
    parser.add_argument("--path", help="Local file path")
    parser.add_argument("--folder", default="wheels", help="Wheel folder")

    args = parser.parse_args()

    manager = WheelS3Manager()

    if args.action == "upload":
        if not args.path:
            print("Provide --path")
            return
        manager.upload_wheel(args.path)

    elif args.action == "download":
        if not args.file:
            print("Provide --file")
            return
        manager.download_wheel(args.file)

    elif args.action == "list":
        wheels = manager.list_wheels()
        for w in wheels:
            print(w)

    elif args.action == "exists":
        if not args.file:
            print("Provide --file")
            return
        print(manager.wheel_exists(args.file))

    elif args.action == "backup":
        manager.backup_all_wheels(args.folder)


if __name__ == "__main__":
    main()