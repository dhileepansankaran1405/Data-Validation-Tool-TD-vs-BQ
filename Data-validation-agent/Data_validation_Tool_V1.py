import json
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor
import uuid
from typing import TYPE_CHECKING

import ibis
import pandas

from data_validation import combiner, consts, metadata, util
from data_validation.config_manager import ConfigManager
from data_validation.query_builder.random_row_builder import RandomRowBuilder
from data_validation.schema_validation import SchemaValidation
from data_validation.validation_builder import ValidationBuilder

if TYPE_CHECKING:
    from ibis.backends.base import BaseBackend


class DataValidation(object):
    def __init__(
        self,
        config,
        validation_builder=None,
        schema_validator=None,
        result_handler=None,
        verbose=False,
        cached_source_client: "BaseBackend" = None,
        cached_target_client: "BaseBackend" = None,
    ):
        self.verbose = verbose
        self._fresh_connections = not bool(
            cached_source_client and cached_target_client
        )

        self.config = config

        self.config_manager = ConfigManager(
            config,
            source_client=cached_source_client,
            target_client=cached_target_client,
            verbose=self.verbose,
        )

        self.run_metadata = metadata.RunMetadata()
        self.run_metadata.labels = self.config_manager.labels
        self.run_metadata.run_id = self.config_manager.run_id or str(uuid.uuid4())

        self.validation_builder = validation_builder or ValidationBuilder(
            self.config_manager
        )

        self.schema_validator = schema_validator or SchemaValidation(
            self.config_manager, run_metadata=self.run_metadata, verbose=self.verbose
        )

        self.result_handler = result_handler or self.config_manager.get_result_handler()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self._fresh_connections and hasattr(self, "config_manager"):
            self.config_manager.close_client_connections()

    def execute(self):
        if self.config_manager.use_random_rows():
            util.timed_call("Random row filter", self._add_random_row_filter)

        if self.config_manager.validation_type == consts.ROW_VALIDATION:
            grouped_fields = self.validation_builder.pop_grouped_fields()
            result_df = self.execute_recursive_validation(
                self.validation_builder, grouped_fields
            )
        elif self.config_manager.validation_type == consts.SCHEMA_VALIDATION:
            result_df = util.timed_call(
                "Schema validation", self.schema_validator.execute
            )
        else:
            result_df = self._execute_validation(self.validation_builder)

        return self.result_handler.execute(result_df)

    def _add_random_row_filter(self):
        if not self.config_manager.primary_keys:
            raise ValueError("Primary Keys are required for Random Row Filters")

        source_pk_column = self.config_manager.primary_keys[0][
            consts.CONFIG_SOURCE_COLUMN
        ]
        target_pk_column = self.config_manager.primary_keys[0][
            consts.CONFIG_TARGET_COLUMN
        ]

        randomRowBuilder = RandomRowBuilder(
            [source_pk_column],
            self.config_manager.random_row_batch_size(),
        )

        if (self.config_manager.validation_type == consts.CUSTOM_QUERY) and (
            self.config_manager.custom_query_type == consts.ROW_VALIDATION.lower()
        ):
            query = randomRowBuilder.compile_custom_query(
                self.config_manager.source_client,
                self.config_manager.source_query,
            )
        else:
            query = randomRowBuilder.compile(
                self.config_manager.source_client,
                self.config_manager.source_schema,
                self.config_manager.source_table,
                self.validation_builder.source_builder,
            )

        binary_conversion_required = False
        if query[source_pk_column].type().is_binary():
            binary_conversion_required = True
            query = query.mutate(
                **{source_pk_column: query[source_pk_column].cast("string")}
            )

        if query[
            source_pk_column
        ].type().is_string() and ValidationBuilder.is_padded_char(
            self.config_manager.source_client,
            self.config_manager.get_source_raw_data_types(),
            source_pk_column,
        ):
            query = query.mutate(**{source_pk_column: query[source_pk_column].rstrip()})

        random_rows = self.config_manager.source_client.execute(query)
        if len(random_rows) == 0:
            return

        source_values = target_values = list(random_rows[source_pk_column])
        if binary_conversion_required:
            target_values = source_values = [
                ibis.literal(_).cast("binary") for _ in source_values
            ]
        elif query[source_pk_column].type().is_string():
            if (
                self.config_manager.source_client.name == "oracle"
                and ValidationBuilder.is_padded_char(
                    self.config_manager.source_client,
                    self.config_manager.get_source_raw_data_types(),
                    source_pk_column,
                )
            ):
                char_length = self.config_manager.get_source_raw_data_types()[
                    source_pk_column
                ][2]
                source_values = [
                    key_value.ljust(char_length) for key_value in source_values
                ]
            if (
                self.config_manager.target_client.name == "oracle"
                and ValidationBuilder.is_padded_char(
                    self.config_manager.target_client,
                    self.config_manager.get_target_raw_data_types(),
                    target_pk_column,
                )
            ):
                char_length = self.config_manager.get_target_raw_data_types()[
                    target_pk_column
                ][2]
                target_values = [
                    key_value.ljust(char_length) for key_value in source_values
                ]

        filter_field = {
            consts.CONFIG_TYPE: consts.FILTER_TYPE_ISIN,
            consts.CONFIG_FILTER_SOURCE_COLUMN: source_pk_column,
            consts.CONFIG_FILTER_SOURCE_VALUE: source_values,
            consts.CONFIG_FILTER_TARGET_COLUMN: target_pk_column,
            consts.CONFIG_FILTER_TARGET_VALUE: target_values,
        }

        self.validation_builder.add_filter(filter_field)

    def query_too_large(self, rows_df, grouped_fields):
        if len(grouped_fields) > 1:
            return False

        try:
            count_df = rows_df[
                rows_df[consts.AGGREGATION_TYPE] == consts.CONFIG_TYPE_COUNT
            ]
            for row in count_df.to_dict(orient="row"):
                recursive_query_size = max(
                    float(row[consts.SOURCE_AGG_VALUE]),
                    float(row[consts.TARGET_AGG_VALUE]),
                )
                if recursive_query_size > self.config_manager.max_recursive_query_size:
                    logging.warning("Query result is too large for recursion: %s", row)
                    return True
        except Exception:
            logging.warning("Recursive values could not be cast to float.")
            return False

        return False

    def execute_recursive_validation(self, validation_builder, grouped_fields):
        past_results = []
        if len(grouped_fields) > 0:
            validation_builder.add_query_group(grouped_fields[0])
            result_df = self._execute_validation(validation_builder)

            for grouped_key in result_df[consts.GROUP_BY_COLUMNS].unique():
                group_suceeded = True
                grouped_key_df = result_df[
                    result_df[consts.GROUP_BY_COLUMNS] == grouped_key
                ]

                if self.query_too_large(grouped_key_df, grouped_fields):
                    past_results.append(grouped_key_df)
                    continue

                for row in grouped_key_df.to_dict(orient="row"):
                    if row[consts.SOURCE_AGG_VALUE] == row[consts.TARGET_AGG_VALUE]:
                        continue
                    else:
                        group_suceeded = False
                        break

                if group_suceeded:
                    past_results.append(grouped_key_df)
                else:
                    recursive_validation_builder = validation_builder.clone()
                    self._add_recursive_validation_filter(
                        recursive_validation_builder, row
                    )
                    past_results.append(
                        self.execute_recursive_validation(
                            recursive_validation_builder, grouped_fields[1:]
                        )
                    )
        elif self.config_manager.primary_keys and len(grouped_fields) == 0:
            past_results.append(self._execute_validation(validation_builder))
        else:
            warnings.warn(
                "WARNING: No Primary Keys Suppplied in Row Validation", UserWarning
            )
            return None

        return pandas.concat(past_results)

    def _add_recursive_validation_filter(self, validation_builder, row):
        group_by_columns = json.loads(row[consts.GROUP_BY_COLUMNS])
        for alias, value in group_by_columns.items():
            filter_field = {
                consts.CONFIG_TYPE: consts.FILTER_TYPE_EQUALS,
                consts.CONFIG_FILTER_SOURCE_COLUMN: validation_builder.get_grouped_alias_source_column(
                    alias
                ),
                consts.CONFIG_FILTER_SOURCE_VALUE: value,
                consts.CONFIG_FILTER_TARGET_COLUMN: validation_builder.get_grouped_alias_target_column(
                    alias
                ),
                consts.CONFIG_FILTER_TARGET_VALUE: value,
            }
            validation_builder.add_filter(filter_field)

    def _execute_validation(self, validation_builder):
        self.run_metadata.validations = validation_builder.get_metadata()

        source_query = validation_builder.get_source_query()
        target_query = validation_builder.get_target_query()

        join_on_fields = (
            set(validation_builder.get_primary_keys())
            if (self.config_manager.validation_type == consts.ROW_VALIDATION)
            or (
                self.config_manager.validation_type == consts.CUSTOM_QUERY
                and self.config_manager.custom_query_type == "row"
            )
            else set(validation_builder.get_group_aliases())
        )

        is_value_comparison = (
            self.config_manager.validation_type == consts.ROW_VALIDATION
            or (
                self.config_manager.validation_type == consts.CUSTOM_QUERY
                and self.config_manager.custom_query_type == "row"
            )
        )

        futures = []
        with ThreadPoolExecutor() as executor:
            futures.append(
                executor.submit(
                    util.timed_call,
                    "Source query",
                    self.config_manager.source