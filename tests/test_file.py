import json
from unittest.mock import patch, mock_open
from utils.file import _load_checkpoint, save_checkpoint

class TestCheckpoint:

    @patch("utils.file.open", new_callable=mock_open, read_data="")
    def test_load_checkpoint(self, mock_file):
        result = _load_checkpoint("dummy_path")
        assert result == (-1, '', '', [], False)
        mock_file.assert_called_once_with("dummy_path", "r", encoding='utf-8')

    @patch(
        "utils.file.open",
        new_callable=mock_open,
        read_data=json.dumps({
            "checkpoint": 1999,
            "schema": "space_product_aies",
            "table": "x_data_mart_irs",
            "columns": [
                ["col1", "character varying"], ["col2", "character varying"],
                ["col3", "bigint"], ["col4", "bigint"]
            ]
        }),
    )
    def test_load_checkpoint_with_data(self, mock_file):
        result = _load_checkpoint("dummy_path")
        expected_result = (
            1999,
            "space_product_aies",
            "x_data_mart_irs",
            [
                ["col1", "character varying"], ["col2", "character varying"],
                ["col3", "bigint"], ["col4", "bigint"]
            ],
            True
        )
        assert result == expected_result
        mock_file.assert_called_once_with("dummy_path", "r", encoding='utf-8')

    @patch("utils.file.open", new_callable=mock_open)
    def test_save_checkpoint(self, mock_file):
        # Test input values
        file_path = "dummy_path"
        checkpoint = 1999
        schema = "space_product_aies"
        table = "x_data_mart_irs"
        columns = [
            ("ref_per", "character varying"),
            ("reporting_id", "character varying"),
            ("ent_id", "bigint"),
            ("ein_num", "bigint")
        ]

        # Expected content in the file
        expected_data = {
            'checkpoint': checkpoint,
            'schema': schema,
            'table': table,
            'columns': columns
        }
        expected_file_content = f"{json.dumps(expected_data)}\n"

        # Call the function
        save_checkpoint(file_path, checkpoint, schema, table, columns)

        # Verify that the correct file was opened
        mock_file.assert_called_once_with(file_path, 'w', encoding='utf-8')

        # Verify the correct data was written to the file
        mock_file().write.assert_called_once_with(expected_file_content)
