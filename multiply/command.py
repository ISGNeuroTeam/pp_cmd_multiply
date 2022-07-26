import numpy as np
import pandas as pd
from otlang.sdk.syntax import Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax


class MultiplyCommand(BaseCommand):
    """
    Make multiplication of two columns of dataframe
    a, b - columns must be multiplied
    | multiply a b - creates a new df

    | multiply a b as c - creates new column "c" in the old df
    """

    syntax = Syntax(
        [
            Positional("first_multiplier", required=True, otl_type=OTLType.ALL),
            Positional("second_multiplier", required=True, otl_type=OTLType.ALL),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress("Start multiply command")
        # that is how you get arguments
        first_multiplier_argument = self.get_arg("first_multiplier")
        if isinstance(first_multiplier_argument.value, str):
            first_multiplier = df[first_multiplier_argument.value]
        else:
            first_multiplier = first_multiplier_argument.value

        second_multiplier_argument = self.get_arg("second_multiplier")
        if isinstance(second_multiplier_argument.value, str):
            second_multiplier = df[second_multiplier_argument.value]
        else:
            second_multiplier = second_multiplier_argument.value
        result_column_name = second_multiplier_argument.named_as

        if isinstance(first_multiplier, (int, float)) and isinstance(second_multiplier, (int, float)):
            if result_column_name != "" and not df.empty:
                first_multiplier = np.array([first_multiplier] * df.shape[0])
                second_multiplier = np.array([second_multiplier] * df.shape[0])
            else:
                first_multiplier = np.array([first_multiplier])
                second_multiplier = np.array([second_multiplier])

        self.logger.debug(f"Command add get first positional argument = {first_multiplier_argument.value}")
        self.logger.debug(
            f"Command add get second positional argument = {second_multiplier_argument.value}"
        )

        if result_column_name != "":
            if not df.empty:
                df[result_column_name] = first_multiplier * second_multiplier
            else:
                df = pd.DataFrame({result_column_name: first_multiplier * second_multiplier})
            self.logger.debug(f"New column name: {result_column_name}")

        else:
            df = pd.DataFrame(
                {
                    f"multiply_{first_multiplier_argument.value}_{second_multiplier_argument.value}": first_multiplier * second_multiplier

                }
            )
        self.log_progress("Multiplication is completed.", stage=1, total_stages=1)
        return df
