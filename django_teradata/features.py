from django.db import InterfaceError
from django.db.backends.base.features import BaseDatabaseFeatures


class DatabaseFeatures(BaseDatabaseFeatures):
    supports_timezones = True
    can_return_rows_from_bulk_insert = False
    has_bulk_insert = False
    supports_paramstyle_pyformat = False
    ignores_unnecessary_order_by_in_subqueries = False
    allows_auto_pk_0 = False
    can_clone_databases = False
    closed_cursor_error_class = InterfaceError
    allows_multiple_constraints_on_same_fields = False
    indexes_foreign_keys = False
    nulls_order_largest = True
    supported_explain_formats = {'TEXT'}
    supports_comments = False
    supports_comments_inline = True
    supports_column_check_constraints = False
    supports_table_check_constraints = False
    supports_expression_indexes = False
    supports_ignore_conflicts = False
    supports_index_column_ordering = False
    supports_json_field = False
    supports_over_clause = True
    supports_partial_indexes = False
    supports_regex_backreferencing = False
    supports_sequence_reset = False
    supports_slicing_ordering_in_compound = True
    supports_subqueries_in_group_by = False
    supports_temporal_subtraction = False
    supports_boolean_expr_in_select_clause = False
    # This really means "supports_nested_transactions". Teradata supports a
    # single level of transaction, BEGIN + (ROLLBACK|COMMIT). Multiple BEGINS
    # contribute to the current (only) transaction.
    supports_transactions = False
    uses_savepoints = False

    test_now_utc_template = 'CURRENT_TIMESTAMP'

