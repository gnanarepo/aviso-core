from config import write_hierarchy_to_gbm


def test_dummy():
    assert 1+1 == 2

def test_insert_and_find_document(test_db):
    test_collection = test_db["users"]
    test_doc = {"name": "Faizan Mazhar", "email": "faizan@example.com"}

    # Insert document
    insert_result = test_collection.insert_one(test_doc)
    assert insert_result.acknowledged is True

    # Retrieve document
    fetched_doc = test_collection.find_one({"_id": insert_result.inserted_id})
    assert fetched_doc is not None
    assert fetched_doc["name"] == "Faizan Mazhar"
    assert fetched_doc["email"] == "faizan@example.com"

    # Delete document
    delete_result = test_collection.delete_one({"_id": insert_result.inserted_id})
    assert delete_result.deleted_count == 1

    fetched_doc = test_collection.find_one({"_id": insert_result.inserted_id})
    assert fetched_doc is None



from unittest.mock import MagicMock, patch

@patch('utils.misc_utils.try_index')
@patch('infra.create_collection_checksum')
@patch('settings.sec_context')
def test_write_hierarchy_to_gbm_mismatch_triggers_sync(mock_sec_ctx, mock_checksum, mock_try_index):
    # Setup
    mock_config = MagicMock(debug=True)

    # Mock DB
    mock_db = MagicMock()
    mock_coll = MagicMock()
    mock_coll.find.return_value = [{'a': 1}]
    mock_db.__getitem__.return_value = mock_coll

    # Checksum mismatch
    mock_sec_ctx.get_microservice_config.return_value = True
    mock_sec_ctx.gbm.api.side_effect = ['remote_checksum', None]
    mock_sec_ctx.tenant_db = mock_db

    mock_checksum.return_value = 'local_checksum'
    mock_try_index.return_value = {'a': 1}

    result = write_hierarchy_to_gbm(config=mock_config)

    assert result is True
    mock_sec_ctx.gbm.api.assert_any_call('/gbm/hier_metadata', None)
    mock_sec_ctx.gbm.api.assert_any_call('/gbm/hier_sync', [{'a': 1}])
