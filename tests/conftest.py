import pytest
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
TEST_DB_NAME = os.getenv("TEST_DB_NAME")


@pytest.fixture(scope="session")
def mongo_client():
    client = MongoClient(os.getenv("MONGO_URI"))
    assert client.admin.command("ping")["ok"] != 0.0
    yield client
    client.drop_database(os.getenv("TEST_DB_NAME"))
    client.close()


@pytest.fixture(scope="function")
def test_db(mongo_client):
    db = mongo_client[TEST_DB_NAME]
    for col in db.list_collection_names():
        db[col].delete_many({})
    return db


def test_db_connection():
    client = MongoClient(MONGO_URI)
    client.admin.command('ping')
    print(f"\n{client.list_database_names()}\n")
