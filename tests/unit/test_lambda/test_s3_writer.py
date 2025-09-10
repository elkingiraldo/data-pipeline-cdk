from unittest.mock import patch, MagicMock
from lambdas.data_extractor.s3_writer import S3Writer

@patch("lambdas.data_extractor.s3_writer.boto3")
def test_write_data_builds_key(mock_boto):
    mock_s3 = MagicMock()
    mock_boto.client.return_value = mock_s3

    w = S3Writer("my-bucket")
    key = w.write_data(
        data=[{"a": 1}],
        prefix="raw-data/year=2024/month=01/day=01",
        format="json",
        metadata={"k": "v"},
    )
    assert key.startswith("raw-data/year=2024/month=01/day=01/")
    mock_s3.put_object.assert_called()
