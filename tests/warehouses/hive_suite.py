from tests.common.sql_test_case import TARGET_HIVE
from tests.common.sql_test_suite import SqlTestSuite
from sodasql.dialects.spark_dialect import ColumnMetadata


class HiveSuite(SqlTestSuite):

    def setUp(self) -> None:
        self.target = TARGET_HIVE
        super().setUp()

    def test_sql_columns_metadata(self):
        data_type = self.dialect.data_type_varchar_255.lower()
        raw_metadata = [
            ColumnMetadata("name", data_type, is_nullable="YES"),
            ColumnMetadata("age", data_type, is_nullable="YES"),
            ColumnMetadata("name", data_type, is_nullable="YES")
        ]

        expected_metadata = [
            ColumnMetadata("name", data_type, is_nullable="YES"),
            ColumnMetadata("age", data_type, is_nullable="YES"),
            ColumnMetadata("name", data_type, is_nullable="YES")
        ]

        self.sql_recreate_table([" ".join(column[:2]) for column in raw_metadata])
        columns_metadata = self.dialect.sql_columns_metadata(
            self.default_test_table_name)

        assert columns_metadata == raw_metadata
