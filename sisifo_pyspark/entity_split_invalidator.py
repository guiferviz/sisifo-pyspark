import sisifo


@sisifo.add_task("pyspark")
class EntitySplitInvalidator(sisifo.Task):
    def __init__(self, entity_valid=None, entity_invalid=None,
                 invalidate_if=None, **kwargs):
        super().__init__(**kwargs)

        invalidate_if = self.create_subtask(invalidate_if)
        if not invalidate_if or self._is_incorrect_task(invalidate_if):
            raise ValueError("Expecting a task with an output entity and an "
                             "output column")
        invalidate_if.column_out = f"_{self.id}_aux_column"
        self.invalidate_if = invalidate_if

        self.entity_valid = entity_valid or f"{invalidate_if.entity_out}_valid"
        self.entity_invalid = entity_invalid or f"{invalidate_if.entity_out}_invalid"

    def _is_incorrect_task(self, task):
        attr = vars(task)
        return "column_out" not in attr or "entity_out" not in attr

    def run(self, data):
        self.invalidate_if.run(data)
        df = data[self.invalidate_if.entity_out]
        col = df[self.invalidate_if.column_out]

        df_invalid = df.filter(col).drop(col)
        df_valid = df.filter(~col).drop(col)

        # Remove temporal column from invalidate_if output.
        data[self.invalidate_if.entity_out] = df.drop(col)
        # Replace entity_valid
        data[self.entity_valid] = df_valid
        # Create or append to entity_invalid
        if self.entity_invalid in data:
            data[self.entity_invalid] = data[self.entity_invalid].union(df_invalid)
        else:
            data[self.entity_invalid] = df_invalid
