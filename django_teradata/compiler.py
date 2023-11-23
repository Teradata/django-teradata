from itertools import chain

from django.core.exceptions import EmptyResultSet, FullResultSet
from django.db import NotSupportedError
from django.db.models.expressions import Ref
from django.db.models.sql import compiler
from django.db.models.sql.constants import GET_ITERATOR_CHUNK_SIZE, MULTI
from django.db.transaction import TransactionManagementError


class SQLCompiler(compiler.SQLCompiler):

    def results_iter(
        self,
        results=None,
        tuple_expected=False,
        chunked_fetch=False,
        chunk_size=GET_ITERATOR_CHUNK_SIZE,
    ):
        """Return an iterator over the results from executing this query."""
        if results is None:
            results = self.execute_sql(
                MULTI, chunked_fetch=chunked_fetch, chunk_size=chunk_size
            )
        fields = [s[0] for s in self.select[0: self.col_count]]
        converters = self.get_converters(fields)
        rows = chain.from_iterable(results)
        if converters:
            rows = self.apply_converters(rows, converters)
        # override to fix the tuple conversion
        if tuple_expected:
            rows = map(tuple, rows)
        return rows

    def as_sql(self, with_limits=True, with_col_aliases=False):
        """
        Create the SQL for this query. Return the SQL string and list of
        parameters.

        If 'with_limits' is False, any limit/offset information is not included
        in the query.

        Teradata does not support LIMIT and OFFSET, it supports TOP
        """
        refcounts_before = self.query.alias_refcount.copy()
        try:
            combinator = self.query.combinator
            extra_select, order_by, group_by = self.pre_sql_setup(
                with_col_aliases=with_col_aliases or bool(combinator),
            )
            for_update_part = None
            # Is a LIMIT/OFFSET clause needed?
            with_limit_offset = with_limits and self.query.is_sliced
            combinator = self.query.combinator
            features = self.connection.features

            # The do_offset flag indicates whether we need to construct
            # the SQL needed to replace limit/offset w/Teradata.
            high_mark = self.query.high_mark
            low_mark = self.query.low_mark
            do_limit = with_limits and high_mark is not None
            do_offset = with_limits and low_mark != 0

            if combinator:
                if not getattr(features, "supports_select_{}".format(combinator)):
                    raise NotSupportedError(
                        "{} is not supported on this database backend.".format(
                            combinator
                        )
                    )
                result, params = self.get_combinator_sql(
                    combinator, self.query.combinator_all
                )
            elif self.qualify:
                result, params = self.get_qualify_sql()
                order_by = None
            else:
                distinct_fields, distinct_params = self.get_distinct()
                # This must come after 'select', 'ordering', and 'distinct'
                # (see docstring of get_from_clause() for details).
                from_, f_params = self.get_from_clause()
                try:
                    where, w_params = (
                        self.compile(self.where) if self.where is not None else ("", [])
                    )
                except EmptyResultSet:
                    if self.elide_empty:
                        raise
                    # Use a predicate that's always False.
                    where, w_params = "0 = 1", []
                except FullResultSet:
                    where, w_params = "", []
                try:
                    having, h_params = (
                        self.compile(self.having)
                        if self.having is not None
                        else ("", [])
                    )
                except FullResultSet:
                    having, h_params = "", []
                result = ["SELECT"]
                params = []

                if self.query.distinct:
                    distinct_result, distinct_params = self.connection.ops.distinct_sql(
                        distinct_fields,
                        distinct_params,
                    )
                    result += distinct_result
                    params += distinct_params

                # Teradata uses TOP instead of LIMIT
                if do_limit and not do_offset:
                    result.append('TOP %d' % high_mark)

                out_cols = []
                col_idx = 1
                for _, (s_sql, s_params), alias in self.select + extra_select:
                    if alias:
                        s_sql = '%s AS %s' % (s_sql, self.connection.ops.quote_name(alias))
                    elif with_col_aliases or do_offset:
                        s_sql = '%s AS %s' % (s_sql, 'Col%d' % col_idx)
                        col_idx += 1
                    params.extend(s_params)
                    out_cols.append(s_sql)

                # Teradata requires an order-by clause for offsetting
                if do_offset:
                    meta = self.query.get_meta()
                    qn = self.quote_name_unless_alias
                    offsetting_order_by = '%s.%s' % (qn(meta.db_table), qn(meta.pk.db_column or meta.pk.column))
                    if order_by:
                        ordering = []
                        for expr, (o_sql, o_params, _) in order_by:
                            # value_expression in OVER clause cannot refer to
                            # expressions or aliases in the select list.
                            src = next(iter(expr.get_source_expressions()))
                            if isinstance(src, Ref):
                                src = next(iter(src.get_source_expressions()))
                                o_sql, _ = src.as_sql(self, self.connection)
                                odir = 'DESC' if expr.descending else 'ASC'
                                o_sql = '%s %s' % (o_sql, odir)
                            ordering.append(o_sql)
                            params.extend(o_params)
                        offsetting_order_by = ', '.join(ordering)
                        order_by = []
                    out_cols.append('ROW_NUMBER() OVER (ORDER BY %s) AS rn' % offsetting_order_by)

                result += [", ".join(out_cols)]
                if from_:
                    result += ["FROM", *from_]
                elif self.connection.features.bare_select_suffix:
                    result += [self.connection.features.bare_select_suffix]
                params.extend(f_params)

                if self.query.select_for_update and features.has_select_for_update:
                    if (
                            self.connection.get_autocommit()
                            # Don't raise an exception when database doesn't
                            # support transactions, as it's a noop.
                            and features.supports_transactions
                    ):
                        raise TransactionManagementError(
                            "select_for_update cannot be used outside of a transaction."
                        )

                    if (
                            with_limit_offset
                            and not features.supports_select_for_update_with_limit
                    ):
                        raise NotSupportedError(
                            "LIMIT/OFFSET is not supported with "
                            "select_for_update on this database backend."
                        )
                    nowait = self.query.select_for_update_nowait
                    skip_locked = self.query.select_for_update_skip_locked
                    of = self.query.select_for_update_of
                    no_key = self.query.select_for_no_key_update
                    # If it's a NOWAIT/SKIP LOCKED/OF/NO KEY query but the
                    # backend doesn't support it, raise NotSupportedError to
                    # prevent a possible deadlock.
                    if nowait and not features.has_select_for_update_nowait:
                        raise NotSupportedError(
                            "NOWAIT is not supported on this database backend."
                        )
                    elif skip_locked and not features.has_select_for_update_skip_locked:
                        raise NotSupportedError(
                            "SKIP LOCKED is not supported on this database backend."
                        )
                    elif of and not features.has_select_for_update_of:
                        raise NotSupportedError(
                            "FOR UPDATE OF is not supported on this database backend."
                        )
                    elif no_key and not features.has_select_for_no_key_update:
                        raise NotSupportedError(
                            "FOR NO KEY UPDATE is not supported on this "
                            "database backend."
                        )
                    for_update_part = self.connection.ops.for_update_sql(
                        nowait=nowait,
                        skip_locked=skip_locked,
                        of=self.get_select_for_update_of_arguments(),
                        no_key=no_key,
                    )

                if for_update_part and features.for_update_after_from:
                    result.append(for_update_part)

                if where:
                    result.append("WHERE %s" % where)
                    params.extend(w_params)

                grouping = []
                for g_sql, g_params in group_by:
                    grouping.append(g_sql)
                    params.extend(g_params)
                if grouping:
                    if distinct_fields:
                        raise NotImplementedError(
                            "annotate() + distinct(fields) is not implemented."
                        )
                    order_by = order_by or self.connection.ops.force_no_ordering()
                    result.append("GROUP BY %s" % ", ".join(grouping))
                    if self._meta_ordering:
                        order_by = None
                if having:
                    result.append("HAVING %s" % having)
                    params.extend(h_params)

            if self.query.explain_info:
                result.insert(
                    0,
                    self.connection.ops.explain_query_prefix(
                        self.query.explain_info.format,
                        **self.query.explain_info.options,
                    ),
                )

            if order_by:
                ordering = []
                for _, (o_sql, o_params, _) in order_by:
                    ordering.append(o_sql)
                    params.extend(o_params)
                order_by_sql = "ORDER BY %s" % ", ".join(ordering)
                if combinator and features.requires_compound_order_by_subquery:
                    result = ["SELECT * FROM (", *result, ")", order_by_sql]
                else:
                    result.append(order_by_sql)

            if do_offset:
                # Construct the final SQL clause, using the initial select SQL
                # obtained above.
                result = ['SELECT * FROM (%s) AS X WHERE X.rn' % ' '.join(result)]
                # Place WHERE condition on `rn` for the desired range.
                if do_limit:
                    result.append('BETWEEN %d AND %d' % (low_mark + 1, high_mark))
                else:
                    result.append('>= %d' % (low_mark + 1))
                if not self.query.subquery:
                    result.append('ORDER BY X.rn')

            if for_update_part and not features.for_update_after_from:
                result.append(for_update_part)

            if self.query.subquery and extra_select:
                # If the query is used as a subquery, the extra selects would
                # result in more columns than the left-hand side expression is
                # expecting. This can happen when a subquery uses a combination
                # of order_by() and distinct(), forcing the ordering expressions
                # to be selected as well. Wrap the query in another subquery
                # to exclude extraneous selects.
                sub_selects = []
                sub_params = []
                for index, (select, _, alias) in enumerate(self.select, start=1):
                    if not alias and with_col_aliases:
                        alias = 'col%d' % index
                    if alias:
                        sub_selects.append("%s.%s" % (
                            self.connection.ops.quote_name('subquery'),
                            self.connection.ops.quote_name(alias),
                        ))
                    else:
                        select_clone = select.relabeled_clone({select.alias: 'subquery'})
                        subselect, subparams = select_clone.as_sql(self, self.connection)
                        sub_selects.append(subselect)
                        sub_params.extend(subparams)
                return "SELECT %s FROM (%s) subquery" % (
                    ", ".join(sub_selects),
                    " ".join(result),
                ), tuple(sub_params + params)

            return " ".join(result), tuple(params)
        finally:
            # Finally do cleanup - get rid of the joins we created above.
            self.query.reset_refcounts(refcounts_before)


#dirty hack for SQLInsertCompiler
class SQLInsertCompiler(compiler.SQLInsertCompiler):
    # verbatim copy
    def as_sql(self):
        # We don't need quote_name_unless_alias() here, since these are all
        # going to be column names (so we can avoid the extra overhead).
        qn = self.connection.ops.quote_name
        opts = self.query.get_meta()
        insert_statement = self.connection.ops.insert_statement(
            on_conflict=self.query.on_conflict,
        )
        result = ["%s %s" % (insert_statement, qn(opts.db_table))]
        fields = self.query.fields or [opts.pk]
        result.append("(%s)" % ", ".join(qn(f.column) for f in fields))

        if self.query.fields:
            value_rows = [
                [
                    self.prepare_value(field, self.pre_save_val(field, obj))
                    for field in fields
                ]
                for obj in self.query.objs
            ]
        else:
            # An empty object.
            value_rows = [
                [self.connection.ops.pk_default_value()] for _ in self.query.objs
            ]
            fields = [None]

        # Currently the backends just accept values when generating bulk
        # queries and generate their own placeholders. Doing that isn't
        # necessary and it should be possible to use placeholders and
        # expressions in bulk inserts too.
        can_bulk = (
            not self.returning_fields and self.connection.features.has_bulk_insert
        )

        placeholder_rows, param_rows = self.assemble_as_sql(fields, value_rows)

        on_conflict_suffix_sql = self.connection.ops.on_conflict_suffix_sql(
            fields,
            self.query.on_conflict,
            (f.column for f in self.query.update_fields),
            (f.column for f in self.query.unique_fields),
        )
        if (
            self.returning_fields
            and self.connection.features.can_return_columns_from_insert
        ):
            if self.connection.features.can_return_rows_from_bulk_insert:
                result.append(
                    self.connection.ops.bulk_insert_sql(fields, placeholder_rows)
                )
                params = param_rows
            else:
                result.append("VALUES (%s)" % ", ".join(placeholder_rows[0]))
                params = [param_rows[0]]
            if on_conflict_suffix_sql:
                result.append(on_conflict_suffix_sql)
            # Skip empty r_sql to allow subclasses to customize behavior for
            # 3rd party backends. Refs #19096.
            r_sql, self.returning_params = self.connection.ops.return_insert_columns(
                self.returning_fields
            )
            if r_sql:
                result.append(r_sql)
                params += [self.returning_params]
            return [(" ".join(result), tuple(chain.from_iterable(params)))]

        if can_bulk:
            result.append(self.connection.ops.bulk_insert_sql(fields, placeholder_rows))
            if on_conflict_suffix_sql:
                result.append(on_conflict_suffix_sql)
            return [(" ".join(result), tuple(p for ps in param_rows for p in ps))]
        else:
            if on_conflict_suffix_sql:
                result.append(on_conflict_suffix_sql)
            return [
                (" ".join(result + ["VALUES (%s)" % ", ".join(p)]), vals)
                for p, vals in zip(placeholder_rows, param_rows)
            ]

SQLInsertCompiler = compiler.SQLInsertCompiler
SQLDeleteCompiler = compiler.SQLDeleteCompiler
SQLUpdateCompiler = compiler.SQLUpdateCompiler
SQLAggregateCompiler = compiler.SQLAggregateCompiler
