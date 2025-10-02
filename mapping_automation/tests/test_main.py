# tests/test_main.py
import json
import unittest
from tkinter.constants import ACTIVE
from unittest.mock import patch, MagicMock
from ..src.main import (get_connection, get_src_info, get_field_info, mapping_audit, append_proposed_fields,
                        get_metadata_elastic_search, elasticsearch_check_from_df, add_finalized_transformation,
                        write_updated_audit_to_excel, canonical_inserts_from_df, origin_inserts_from_df,
                        canonical_updates_from_df, origin_updates_from_df)
from requests.exceptions import RequestException
import pandas as pd
from openpyxl.utils import get_column_letter
import re


class TestDBConnection(unittest.TestCase):

    @patch('Automation_Scripts.mapping_automation.src.main.psycopg2.pool.SimpleConnectionPool')
    def test_get_connection(self, mock_pool_class):
        # Arrange: create a mock pool object
        mock_pool_instance = MagicMock()
        mock_pool_class.return_value = mock_pool_instance
        mock_conn = MagicMock()
        mock_pool_instance.getconn.return_value = mock_conn

        # Act: call get_connection
        conn = get_connection()

        # Assert: getconn was called and returned the mock connection
        mock_pool_instance.getconn.assert_called_once()
        self.assertEqual(conn, mock_conn)

class TestGetSrcInfo(unittest.TestCase):

    def test_get_src_info_returns_expected(self):
        # Arrange
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ('SRC_A', 'REST', 'Provider1', 1, 'Dataset1', 'Desc1', 'agent')
        ]
        src_list = ['SRC_A']
        dl_type = 'agent'

        # Act
        result = get_src_info(mock_cursor, src_list, dl_type)

        # Assert
        mock_cursor.execute.assert_called_once()  # check that execute was called
        self.assertEqual(result, [
            ('SRC_A', 'REST', 'Provider1', 1, 'Dataset1', 'Desc1', 'agent')
        ])

class TestGetFieldInfo(unittest.TestCase):

    def test_get_field_info_expected_results(self):
        # Arrange
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            'sample_Id', 'sample_field_name'
        ]
        test_field = ['mapping_field']
        dl_type = 'listings'

        # Act
        result = get_field_info(mock_cursor, test_field, dl_type)

        # Assert
        mock_cursor.execute.assert_called_once()
        self.assertEqual(result, ['sample_Id', 'sample_field_name'])

class TestMappingAudit(unittest.TestCase):
    def test_mapping_audit(self):
        # Arrange
        mock_cursor = MagicMock()

        sample_tuple = (
            'SRC_A',      # Source
            'REST',       # Protocol
            'Provider1',  # Provider
            1,            # Dataset ID
            'Dataset1',   # Class
            'Desc1',      # Class Description
            'agent',      # Download Type
            101,          # Field ID
            'SomeField'   # Canonical Field Name
        )

        test_cases = [
            # (fetchall return value, expected status)
            ([], 'Not Mapped'),
            ([(True,)], 'Mapped'),
            ([(False,)], 'Deactivated')
        ]

        for fetch_val, expected_status in test_cases:
            with self.subTest(fetch_val=fetch_val):
                mock_cursor.fetchall.return_value = fetch_val

                # Act
                result = mapping_audit(mock_cursor, [sample_tuple])

                # Assert
                mock_cursor.execute.assert_called()  # called at least once
                self.assertEqual(result, [sample_tuple + (expected_status,)])

class TestAppendProposedFields(unittest.TestCase):
    def test_append_proposed_fields_basic(self):
        # Arrange: mock audit data and field mapping definitions
        audit_data = [
            ('SRC_A', 'REST', 'Provider1', 1, 'Dataset1', 'Desc1', 'agent', 101, 'IS_ACTIVE', 'Not Mapped'),
            ('SRC_B', 'WEBAPI', 'Provider2', 2, 'Dataset2', 'Desc2', 'office', 102, 'UNKNOWN_FIELD', 'Mapped')
        ]
        field_mapping_definitions = {
            "IS_ACTIVE": {
                "long_name": "StatusFlag",
                "transformation": "IF(StatusFlag='Active',1,0)"
            }
        }

        # Act: run the function
        result = append_proposed_fields(audit_data, field_mapping_definitions)

        # Assert: check that the correct fields were appended
        expected = [
            ('SRC_A', 'REST', 'Provider1', 1, 'Dataset1', 'Desc1', 'agent', 101, 'IS_ACTIVE', 'Not Mapped',
             'StatusFlag', "IF(StatusFlag='Active',1,0)"),
            ('SRC_B', 'WEBAPI', 'Provider2', 2, 'Dataset2', 'Desc2', 'office', 102, 'UNKNOWN_FIELD', 'Mapped',
             'N/A', 'N/A')
        ]
        self.assertEqual(result, expected)


class TestGetMetadataElasticSearch(unittest.TestCase):

    @patch("Automation_Scripts.mapping_automation.src.main.requests.get")  # Mock requests.get in your module
    def test_successful_response(self, mock_get):
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = {"hits": {"hits": [{"_source": {"tableSystemName": "tbl_name"}}]}}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        source = "SRC_A"
        dataset_name = "Dataset1"
        field_name = "Field1"
        resource = "Property"
        auth_url = "https://fake-url.com"

        # Act
        result = get_metadata_elastic_search(source, dataset_name, field_name, resource, auth_url)

        # Assert
        expected_query = {
            "_source": ["documentId", "className", "longName", "tableSystemName"],
            "query": {
                "bool": {
                    "must": [
                        {"term": {"documentId": {"value": source.lower()}}},
                        {"term": {"className": {"value": dataset_name.lower()}}},
                        {"term": {"resource": {"value": resource.lower()}}},  # insert happens here
                        {"match_phrase": {"longName": field_name}}
                    ]
                }
            }
        }
        mock_get.assert_called_once_with(auth_url, headers={"Content-Type": "application/json"}, data=json.dumps(expected_query))
        self.assertEqual(result, {"hits": {"hits": [{"_source": {"tableSystemName": "tbl_name"}}]}})

    @patch("Automation_Scripts.mapping_automation.src.main.requests.get")
    def test_request_exception(self, mock_get):
        # Arrange
        mock_get.side_effect = RequestException("Network error")

        source = "SRC_A"
        dataset_name = "Dataset1"
        field_name = "Field1"
        resource = None
        auth_url = "https://fake-url.com"

        # Act
        result = get_metadata_elastic_search(source, dataset_name, field_name, resource, auth_url)

        # Assert
        self.assertIn("error", result)
        self.assertIn("Network error", result["error"])

class TestElasticsearchCheckFromDf(unittest.TestCase):

    def setUp(self):
        self.base_df = pd.DataFrame([{
            "Source": "SRC_A",
            "Protocol": "RETS",
            "Provider": "Provider1",
            "Dataset ID": 1,
            "Class": "Dataset1",
            "Class Description": "Desc1",
            "Download Type": "listing",
            "Field ID": 101,
            "Canonical Field Name": "SomeField",
            "Mapping Status": "Not Mapped",
            "Proposed Field Short Name": "Field1"
        }])

    def test_mapped_row_sets_na(self):
        df = self.base_df.copy()
        df["Mapping Status"] = "Mapped"

        result_df = elasticsearch_check_from_df(df, "http://fake-url")
        self.assertEqual(result_df.iloc[0]["es_Pass"], "N/A")
        self.assertEqual(result_df.iloc[0]["Proposed Fields Long Name"], "N/A")

    def test_no_proposed_fields_sets_nf(self):
        df = self.base_df.copy()
        df["Proposed Field Short Name"] = None

        result_df = elasticsearch_check_from_df(df, "http://fake-url")
        self.assertEqual(result_df.iloc[0]["es_Pass"], "N")
        self.assertEqual(result_df.iloc[0]["Proposed Fields Long Name"], "NF")

    @patch("Automation_Scripts.mapping_automation.src.main.get_metadata_elastic_search")
    def test_with_fields_all_found_sets_y(self, mock_meta):
        mock_meta.return_value = {"hits": {"hits": [{"_source": {"tableSystemName": "tbl"}}]}}
        result_df = elasticsearch_check_from_df(self.base_df, "http://fake-url")
        self.assertEqual(result_df.iloc[0]["es_Pass"], "Y")
        self.assertEqual(result_df.iloc[0]["Proposed Fields Long Name"], "tbl")
        mock_meta.assert_called_once()

    @patch("Automation_Scripts.mapping_automation.src.main.get_metadata_elastic_search")
    def test_multiple_fields_first_missing_sets_n(self, mock_meta):
        mock_meta.side_effect = [
            {"hits": {"hits": []}},  # first field fails
        ]
        df = self.base_df.copy()
        df["Proposed Field Short Name"] = "Field1"  # single field
        result_df = elasticsearch_check_from_df(df, "http://fake-url")
        self.assertEqual(result_df.iloc[0]["es_Pass"], "N")
        self.assertIn("NF", result_df.iloc[0]["Proposed Fields Long Name"])

    @patch("Automation_Scripts.mapping_automation.src.main.get_metadata_elastic_search")
    def test_multiple_fields_some_missing_sets_n(self, mock_meta):
        # Arrange: simulate two proposed fields
        df = self.base_df.copy()
        df["Proposed Field Short Name"] = "Field1, Field2"

        # First field found, second not found
        mock_meta.side_effect = [
            {"hits": {"hits": [{"_source": {"tableSystemName": "tbl1"}}]}},  # Field1 hit
            {"hits": {"hits": []}}  # Field2 miss
        ]

        # Act
        result_df = elasticsearch_check_from_df(df, "http://fake-url")

        # Assert
        self.assertEqual(result_df.iloc[0]["es_Pass"], "N")  # because one miss
        self.assertEqual(result_df.iloc[0]["Proposed Fields Long Name"], "tbl1,NF")
        self.assertEqual(mock_meta.call_count, 2)

    @patch("Automation_Scripts.mapping_automation.src.main.get_metadata_elastic_search")
    def test_multiple_fields_all_missing_sets_n(self, mock_meta):
        # Arrange: simulate two proposed fields
        df = self.base_df.copy()
        df["Proposed Field Short Name"] = "Field1, Field2"

        # Both fields return no hits
        mock_meta.side_effect = [
            {"hits": {"hits": []}},  # Field1 miss
            {"hits": {"hits": []}}   # Field2 miss
        ]

        # Act
        result_df = elasticsearch_check_from_df(df, "http://fake-url")

        # Assert
        # Expect es_Pass to be 'N' because neither field was found
        self.assertEqual(result_df.iloc[0]["es_Pass"], "N")
        # Long names should be NF for both
        self.assertEqual(result_df.iloc[0]["Proposed Fields Long Name"], "NF,NF")
        # Make sure both fields were checked
        self.assertEqual(mock_meta.call_count, 2)

    @patch("Automation_Scripts.mapping_automation.src.main.get_metadata_elastic_search")
    def test_resource_mapping_variants(self, mock_meta):
        mock_meta.return_value = {"hits": {"hits": [{"_source": {"tableSystemName": "tbl"}}]}}

        cases = [
            # download_type, protocol, expected_resource
            ("listing", "RETS", "Property"),
            ("listing", "WEBAPI", "EntityType"),
            ("openhouse", "RETS", "OpenHouse"),
            ("openhouse", "WEBAPI", "EntityType"),
            ("agent", "RETS", None),
            ("office", "WEBAPI", None),
            ("random_type", "RETS", ""),  # falls through to default else
        ]

        for dl_type, protocol, expected_resource in cases:
            df = self.base_df.copy()
            df["Download Type"] = dl_type
            df["Protocol"] = protocol

            elasticsearch_check_from_df(df, "http://fake-url")

            # Grab the actual call args to verify resource value
            called_args, called_kwargs = mock_meta.call_args
            _, _, _, actual_resource, _ = called_args  # unpack params
            self.assertEqual(actual_resource, expected_resource)

            mock_meta.reset_mock()

class TestAddFinalizedTransformation(unittest.TestCase):

    def setUp(self):
        self.base_df = pd.DataFrame([{
            "Proposed Field Short Name": "Field1",
            "Proposed Fields Long Name": "tbl_Field1",
            "Proposed Transformation": "concat(Field1, ''='', ''sample replacement'')",
            "es_Pass": "Y"
        }])

    def test_es_pass_not_y_sets_na(self):
        df = self.base_df.copy()
        df["es_Pass"] = "N"

        result_df = add_finalized_transformation(df)
        self.assertEqual(result_df.iloc[0]["Finalized Transformation"], "N/A")

    def test_single_field_replacement(self):
        result_df = add_finalized_transformation(self.base_df)
        self.assertEqual(result_df.iloc[0]["Finalized Transformation"],
                         "concat(tbl_Field1, ''='', ''sample replacement'')")

    def test_multiple_fields_replacement(self):
        df = pd.DataFrame([{
            "Proposed Field Short Name": "Field1, Field2",
            "Proposed Fields Long Name": "tbl1_Field1, tbl2_Field2",
            "Proposed Transformation": "concat(Field1, ''+'', Field2)",
            "es_Pass": "Y"
        }])
        result_df = add_finalized_transformation(df)
        self.assertEqual(result_df.iloc[0]["Finalized Transformation"],
                         "concat(tbl1_Field1, ''+'', tbl2_Field2)")


class TestWriteUpdatedAuditToExcel(unittest.TestCase):

    @patch("Automation_Scripts.mapping_automation.src.main.Workbook")
    def test_write_updated_audit_to_excel_calls_save(self, MockWorkbook):
        # Arrange
        headers = ["Col1", "Col2"]
        rows = [[1, 2], [3, 4]]
        file_path = "dummy.xlsx"

        # Create mock workbook and worksheet
        mock_wb = MockWorkbook.return_value
        mock_ws = mock_wb.active
        mock_ws.append = MagicMock()
        mock_ws.add_table = MagicMock()

        # Mock column_dimensions so KeyError does not occur
        mock_ws.column_dimensions = {}
        for idx in range(1, len(headers) + 1):
            mock_ws.column_dimensions[get_column_letter(idx)] = MagicMock()

        # Act
        write_updated_audit_to_excel(headers, rows, file_path)

        # Assert headers and rows appended
        mock_ws.append.assert_any_call(headers)
        for row in rows:
            mock_ws.append.assert_any_call(row)

        # Assert save called once with the correct file path
        mock_wb.save.assert_called_once_with(file_path)

class TestCanonicalInsertsFromDF(unittest.TestCase):

    @patch("builtins.print")
    def test_canonical_inserts_from_df_generates_inserts(self, mock_print):
        # Arrange
        df = pd.DataFrame([
            {"Field ID": 1, "Dataset ID": 10, "Class": "ClassA", "Class Description": "DescA", "Finalized Transformation": "mapA"},
            {"Field ID": 2, "Dataset ID": 20, "Class": "ClassB", "Class Description": "DescB", "Finalized Transformation": "mapB"},
        ])

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value

        mock_cursor.fetchone.side_effect = [None, (1,)]

        # Act
        canonical_inserts_from_df(df, mock_conn, "TEST_DOWNLOAD")

        # Assert
        execute_calls = [call.args[0] for call in mock_cursor.execute.call_args_list]

        # One INSERT should have been executed
        insert_calls = [stmt for stmt in execute_calls if stmt.strip().startswith("INSERT INTO")]
        self.assertEqual(len(insert_calls), 1, "Expected only one INSERT to be executed")

        insert_sql = insert_calls[0]

        # Use regex to validate VALUES section structure
        self.assertRegex(insert_sql, r"VALUES\s*\(\s*1\s*,", "Expected first value in VALUES to be 1 (field_id)")
        self.assertRegex(insert_sql, r"VALUES\s*\(\s*1\s*,\s*10\s*,", "Expected second value in VALUES to be 10 (dataset_id)")

        # Ensure mapping value is included
        self.assertIn("'mapA'", insert_sql)

        # Now check print statements (but only for the summary, not the SQL itself)
        printed_statements = [call.args[0] for call in mock_print.call_args_list]
        self.assertIn("Canonical Inserts Created", printed_statements[-1])

        # Ensure commit was called
        mock_conn.commit.assert_called_once()

    @patch("builtins.print")
    def test_canonical_inserts_from_df_no_inserts(self, mock_print):
        # Arrange
        df = pd.DataFrame([
            {"Field ID": 1, "Dataset ID": 10, "Class": "ClassA", "Class Description": "DescA", "Finalized Transformation": "mapA"},
        ])

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value

        # Simulate that row already exists
        mock_cursor.fetchone.return_value = (1,)

        # Act
        canonical_inserts_from_df(df, mock_conn, "TEST_DOWNLOAD")

        # Assert SELECT query executed
        mock_cursor.execute.assert_called_once()

        # Assert no insert statement printed
        printed_statements = [call.args[0] for call in mock_print.call_args_list]
        self.assertFalse(any(stmt.strip().startswith("INSERT INTO") for stmt in printed_statements))

        # Assert the summary printout is correct for "no inserts"
        self.assertIn("No new canonical inserts created", printed_statements[-1])

        # Also ensure "Canonical Inserts Created" did not appear
        self.assertNotIn("Canonical Inserts Created", printed_statements)


class TestOriginInsertsFromDF(unittest.TestCase):

    @patch("builtins.print")
    def test_origin_inserts_from_df_fetchall_empty(self, mock_print):
        # Arrange
        df = pd.DataFrame([
            {"Field ID": 1, "Dataset ID": 10, "Class": "ClassA",
             "Proposed Fields Long Name": "Sample Field Full",
             "Proposed Field Short Name": "SampleFIeld"}
        ])

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchall.return_value = []

        # Act
        origin_inserts_from_df(df, mock_conn)

        # Assert
        self.assertEqual(mock_cursor.execute.call_count, 1)

        printed_statements = [call.args[0] for call in mock_print.call_args_list]
        self.assertTrue(any("No mapping IDs found for field_id=1, dataset_id=10" in stmt
                            for stmt in printed_statements))
        self.assertIn("No new origin inserts created", printed_statements[-1])

    @patch("builtins.print")
    def test_origin_inserts_from_df_fetchall_result_no_db_class(self, mock_print):

        # Arrange
        df = pd.DataFrame([
            {"Field ID": 1, "Dataset ID": 10, "Class": "ClassA",
             "Proposed Fields Long Name": "LongName",
             "Proposed Field Short Name": "ShortName"},
        ])

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value

        mock_cursor.fetchall.return_value = [(123, "OtherClass")]

        # Act
        origin_inserts_from_df(df, mock_conn)

        # Assert
        printed_statements = [call.args[0] for call in mock_print.call_args_list]

        self.assertFalse(any("INSERT INTO table_origin_field" in stmt
                             for stmt in printed_statements))
        self.assertTrue(any("No matching dataset found" in stmt for stmt in printed_statements))
        self.assertEqual(mock_cursor.execute.call_count, 1)
        self.assertIn("No new origin inserts created", printed_statements[-1])

    @patch("builtins.print")
    def test_origin_inserts_from_df_fetchall_result_yes_db_class_origin_exists(self, mock_print):

        # Arrange
        df = pd.DataFrame([
            {
                "Field ID": 1,
                "Dataset ID": 10,
                "Class": "ClassA",  # dataset_name
                "Proposed Fields Long Name": "LongName",
                "Proposed Field Short Name": "ShortName"
            }
        ])

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value

        mock_cursor.fetchall.return_value = [(123, "ClassA")]
        mock_cursor.fetchone.return_value = [(1,)]

        # Act
        origin_inserts_from_df(df, mock_conn)

        # Assert
        printed_statements = [call.args[0] for call in mock_print.call_args_list]

        # No INSERT statements should have been generated
        self.assertFalse(any("INSERT INTO table_origin_field" in stmt for stmt in printed_statements))

        # Check that the "Skipping existing origin field" message was printed
        self.assertTrue(any("Skipping existing origin field" in stmt for stmt in printed_statements))

        # Check execute calls
        execute_calls = [call.args[0] for call in mock_cursor.execute.call_args_list]
        self.assertEqual(len(execute_calls), 2)
        self.assertIn("SELECT id, dataset_name FROM table_mapping", execute_calls[0])
        self.assertIn("SELECT 1 FROM table_origin_field", execute_calls[1])
        self.assertIn("No new origin inserts created", printed_statements[-1])

    @patch("builtins.print")
    def test_origin_inserts_from_df_fetchall_result_yes_db_class_origin_dont_exist(self, mock_print):
        # Arrange
        df = pd.DataFrame([
            {
                "Field ID": 1,
                "Dataset ID": 10,
                "Class": "ClassA",  # dataset_name
                "Proposed Fields Long Name": "LongName",
                "Proposed Field Short Name": "ShortName"
            }
        ])

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value

        mock_cursor.fetchall.return_value = [(123, "ClassA")]
        mock_cursor.fetchone.return_value = None

        # Act
        origin_inserts_from_df(df, mock_conn)

        # Assert

        # Check execute calls
        execute_calls = [call.args[0] for call in mock_cursor.execute.call_args_list]
        self.assertEqual(len(execute_calls), 3)
        self.assertIn("SELECT id, dataset_name FROM table_mapping", execute_calls[0])
        self.assertIn("SELECT 1 FROM table_origin_field", execute_calls[1])
        self.assertIn("INSERT INTO table_origin_field",execute_calls[2])

        mock_conn.commit.assert_called_once()

        # Check last print output
        printed_statements = [call.args[0] for call in mock_print.call_args_list]
        self.assertTrue(any("Origin Inserts Created" in stmt for stmt in printed_statements))

    @patch("builtins.print")
    def test_origin_inserts_from_df_fetchall_result_yes_db_class_origin_exist_mixed(self, mock_print):

        # Arrange
        df = pd.DataFrame([{
            "Field ID": 1,
            "Dataset ID": 10,
            "Class": "ClassA",
            "Proposed Fields Long Name": "LongName1, LongName2",
            "Proposed Field Short Name": "ShortName1, ShortName2"
        }])

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value

        mock_cursor.fetchall.return_value = [(123, "ClassA")]
        mock_cursor.fetchone.side_effect = [(1,), None]

        # Act
        origin_inserts_from_df(df, mock_conn)

        # Assert
        printed_statements = [call.args[0] for call in mock_print.call_args_list]
        self.assertIn("Origin Inserts Created", printed_statements[-1])

        self.assertTrue(
            any("Skipping existing origin field" in stmt for stmt in printed_statements),
            "Expected at least one 'Skipping existing origin field' message"
        )

        execute_calls = [call.args[0] for call in mock_cursor.execute.call_args_list]

        # SELECT mapping query
        self.assertIn("SELECT id, dataset_name FROM table_mapping", execute_calls[0])

        # SELECT origin field existence queries
        self.assertIn("SELECT 1 FROM table_origin_field", execute_calls[1])

        # INSERT executed only for non-existing origin fields
        insert_calls = [stmt for stmt in execute_calls if stmt.strip().startswith("INSERT INTO")]
        self.assertEqual(len(insert_calls), 1, "Expected exactly one INSERT executed")

class TestCanonicalUpdateFromDf(unittest.TestCase):

    def test_query_construction(self):

        # Arrange
        df = pd.DataFrame([{
            "Field ID": 1,
            "Dataset ID": 10,
            "Class": "ClassA",
            "Download Type": "sample_download",
            "Finalized Transformation": "sample_transformation"
        }])

        sample_qry = f"""
        SELECT custom_transformation FROM table_mapping
        WHERE field_id = 1
        AND dataset_id = 10
        AND dataset_name = 'ClassA'
        AND download_type = 'sample_download';
        """

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value

        # Act
        canonical_updates_from_df(df, mock_conn)

        # Assert
        executed_query = mock_cursor.execute.call_args_list[0].args[0]
        self.assertEqual(executed_query.strip(), sample_qry.strip())

    @patch("builtins.print")
    def test_fetchone_false(self, mock_print):

        # Arrange
        df = pd.DataFrame([{
            "Field ID": 1,
            "Dataset ID": 10,
            "Class": "ClassA",
            "Download Type": "sample_download",
            "Finalized Transformation": "sample_transformation"
        }])

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchone.return_value = None

        # Act
        canonical_updates_from_df(df, mock_conn)

        # Assert
        printed_statements = [call.args[0] for call in mock_print.call_args_list]
        self.assertIn("Mapping not found", printed_statements)
        self.assertIn("No updates executed", printed_statements[-1])

    @patch("builtins.print")
    def test_fetchone_true_transformation_match(self, mock_print):

        # Arrange
        df = pd.DataFrame([{
            "Field ID": 1,
            "Dataset ID": 10,
            "Class": "ClassA",
            "Download Type": "sample_download",
            "Finalized Transformation": "sample_transformation"
        }])

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchone.return_value = ("sample_transformation",)

        # Act
        canonical_updates_from_df(df, mock_conn)

        # Assert
        execute_calls = [call.args[0] for call in mock_cursor.execute.call_args_list]
        # 0 = SELECT, 1 = UPDATE
        executed_update = execute_calls[1].strip()

        self.assertTrue("UPDATE table_mapping" in executed_update)
        self.assertTrue("field_id = 1" in executed_update)
        self.assertTrue("dataset_id = 10" in executed_update)
        self.assertTrue("SET is_active = true" in executed_update)
        self.assertTrue("last_update_ts = CURRENT_TIMESTAMP" in executed_update)

        # Check commit called
        mock_conn.commit.assert_called_once()

        # Check the print statement
        printed_statements = [call.args[0] for call in mock_print.call_args_list]
        self.assertIn("Canonical Updates Created", printed_statements[-1])

    @patch("builtins.print")
    def test_fetchone_true_transformation_not_match(self, mock_print):

        # Arrange
        df = pd.DataFrame([{
            "Field ID": 1,
            "Dataset ID": 10,
            "Class": "ClassA",
            "Download Type": "sample_download",
            "Finalized Transformation": "sample_transformation"
        }])

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchone.return_value = ("other_transformation",)

        # Act
        canonical_updates_from_df(df, mock_conn)

        # Assert
        execute_calls = [call.args[0] for call in mock_cursor.execute.call_args_list]
        executed_update = execute_calls[1].strip()

        self.assertTrue("UPDATE table_mapping" in executed_update)
        self.assertTrue("SET custom_transformation = 'other_transformation'")
        self.assertTrue("field_id = 1" in executed_update)
        self.assertTrue("dataset_id = 10" in executed_update)
        self.assertTrue("is_active = true" in executed_update)
        self.assertTrue("last_update_ts = CURRENT_TIMESTAMP" in executed_update)

        # Check commit called
        mock_conn.commit.assert_called_once()

        # Check the print statement
        printed_statements = [call.args[0] for call in mock_print.call_args_list]
        self.assertIn("Canonical Updates Created", printed_statements[-1])

class TestOriginUpdateFromDf(unittest.TestCase):

    # def test_query_construction(self):
    #
    #     # Arrange
    #     df = pd.DataFrame([{
    #         "Field ID" : 1,
    #         "Dataset ID" : 10,
    #         "Proposed Fields Short Name" : "sample_field_name",
    #         "Proposed Fields Long Name" : "sample_field_name_2"
    #     }])
    #
    #     sample_qry = f"""
    #     SELECT id FROM table_mapping
    #     WHERE field_id = 1 AND dataset_id = 10;
    #     """
    #
    #     mock_conn = MagicMock()
    #     mock_cursor = mock_conn.cursor.return_value
    #
    #     # Act
    #     origin_updates_from_df(df, mock_conn)
    #
    #     # Assert
    #     executed_query = mock_cursor.execute.call_args_list[0].args[0]
    #     self.assertEqual(executed_query.strip(), sample_qry.strip())

    # @patch("builtins.print")
    # def test_fetchone_false(self, mock_print):
    #
    #     # Arrange
    #     df = pd.DataFrame([{
    #         "Field ID" : 1,
    #         "Dataset ID" : 10,
    #         "Proposed Fields Short Name" : "sample_field_name",
    #         "Proposed Fields Long Name" : "sample_field_name_2"
    #     }])
    #
    #     mock_conn = MagicMock()
    #     mock_cursor = mock_conn.cursor.return_value
    #     mock_cursor.fetchone.return_value = None
    #
    #     # Act
    #     origin_updates_from_df(df, mock_conn)
    #
    #     # Assert
    #     printed_statements = [call.args[0] for call in mock_print.call_args_list]
    #     self.assertIn("Mapping ID not found for field_id=1, dataset_id=10", printed_statements)
    #     self.assertIn("No updates executed", printed_statements[-1])

    # @patch("builtins.print")
    # def test_origin_updates_from_df_row_exists_triggers_update(self, mock_print):
    #
    #     # Arrange
    #     df = pd.DataFrame([{
    #         "Field ID": 1,
    #         "Dataset ID": 10,
    #         "Proposed Fields Short Name": "ShortName",
    #         "Proposed Fields Long Name": "LongName"
    #     }])
    #
    #     mock_conn = MagicMock()
    #     mock_cursor = mock_conn.cursor.return_value
    #
    #     mock_cursor.fetchone.side_effect = [
    #         (123,),  # mapping_id exists
    #         (1,)     # origin field exists → triggers UPDATE
    #     ]
    #
    #     # Act
    #     origin_updates_from_df(df, mock_conn)
    #
    #     # Assert
    #     execute_calls = [call.args[0] for call in mock_cursor.execute.call_args_list]
    #
    #     self.assertIn("SELECT id FROM table_mapping", execute_calls[0])
    #     self.assertIn( "SELECT 1 FROM table_origin_field", execute_calls[1])
    #     self.assertIn( "UPDATE table_origin_field", execute_calls[2])
    #     self.assertIn("mapping_id = 123", execute_calls[2])
    #     self.assertIn("source_field = 'ShortName'", execute_calls[2])
    #     self.assertIn("dataset_id = 10", execute_calls[2])
    #
    #     mock_conn.commit.assert_called_once()
    #
    #     printed_statements = [call.args[0] for call in mock_print.call_args_list]
    #     self.assertIn("Origin Updates Created", printed_statements[-1])

    @patch("builtins.print")
    def test_origin_updates_from_df_row_does_not_exist_triggers_update(self, mock_print):
        # Arrange
        df = pd.DataFrame([{
            "Field ID": 1,
            "Dataset ID": 10,
            "Proposed Fields Short Name": "ShortName",
            "Proposed Fields Long Name": "LongName"
        }])

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchone.side_effect = [
            (123,),  # mapping_id exists
            None    # origin field does not exist → triggers INSERT
        ]

        # Act
        origin_updates_from_df(df, mock_conn)

        # Assert
        execute_calls = [call.args[0] for call in mock_cursor.execute.call_args_list]

        self.assertIn("SELECT id FROM table_mapping", execute_calls[0])
        self.assertIn( "SELECT 1 FROM table_origin_field", execute_calls[1])
        self.assertIn( "INSERT INTO table_origin_field", execute_calls[2])
        self.assertIn("VALUES (123, 'ShortName', 10, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'ShortName', 'LongName')", execute_calls[2])


if __name__ == "__main__":
    unittest.main()
