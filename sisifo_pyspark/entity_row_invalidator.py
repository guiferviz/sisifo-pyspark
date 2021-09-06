import pyspark.sql.functions as F

import sisifo


@sisifo.add_task("pyspark")
class EntityRowInvalidator(sisifo.Task):
    def __init__(self, invalidate_if=None,
                 valid_bool_column=None, log_array_columns=None, **kwargs):
        super().__init__(**kwargs)

        invalidate_if = self.create_subtask(invalidate_if)
        if not invalidate_if or self._is_incorrect_task(invalidate_if):
            raise ValueError("Expecting a task with an output entity and an "
                             "output column")
        invalidate_if.column_out = f"_{self.id}_aux_column"
        self.invalidate_if = invalidate_if

        if not valid_bool_column:
            raise ValueError("Valid bool column must have a valid value")
        self.valid_bool_column = valid_bool_column

        if not log_array_columns:
            log_array_columns = {}
        if type(log_array_columns) != dict:
            raise ValueError("Log array columns should be a dict with "
                             "name columns as a key and messages as a value")
        self.log_array_columns = log_array_columns

    def _is_incorrect_task(self, invalidate_if):
        attr = vars(invalidate_if)
        return "column_out" not in attr or "entity_out" not in attr

    def run(self, data):
        task = self.invalidate_if
        task.run(data)
        df = data[task.entity_out]
        df = self.log_task(df, task.column_out)
        data[task.entity_out] = df.drop(task.column_out)

    def log_task(self, df, is_invalid_column):
        is_invalid_column = F.col(is_invalid_column)
        df = self._invalidate_if(df, is_invalid_column)
        df = self._log_if(df, is_invalid_column)
        return df

    def _invalidate_if(self, df, is_invalid_column):
        df = (
            df.withColumn(self.valid_bool_column,
                          F.when(is_invalid_column,
                                 F.lit(False))
                          .otherwise(
                                 F.col(self.valid_bool_column)))
        )
        return df

    def _log_if(self, df, is_invalid_column):
        for column, message in self.log_array_columns.items():
            df = (
                df.withColumn(column,
                              F.when(is_invalid_column,
                                     self._append_message_to_tasks_applied(column, message))
                              .otherwise(
                                     F.col(column)))
            )
        return df

    def _append_message_to_tasks_applied(self, column, message):
        message = self._format_log_message(message)

        existing_array = F.col(column)
        empty_array = F.array().cast("array<string>")
        existing_or_empty_array = F.coalesce(existing_array, empty_array)
        log_msg_array = F.array(F.lit(message)).cast("array<string>")

        return F.concat(existing_or_empty_array, log_msg_array)

    def _format_log_message(self, message):
        msg = message.format(**vars(self))
        return msg
