from datetime import date, datetime
from decimal import Decimal
import numpy as np
import pandas as pd


def inspect_columns_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    result = []

    numeric_types = (int, float, np.integer, np.floating, Decimal)
    boolean_types = (bool, np.bool_)

    def utf16_units(text: str) -> int:

        return len(text.encode("utf-16-le")) // 2

    for column in df.columns:
        series = df[column]
        non_null = series.dropna()
        values = non_null.tolist()

        missing_count = int(series.isna().sum())
        min_value = None
        max_value = None
        max_text_length = None
        max_nvarchar_units = None

        if not values:
            sql_type = "نیازمند تعیین دستی"

        elif all(isinstance(v, boolean_types) for v in values):
            sql_type = "BIT"

        elif pd.api.types.is_datetime64_any_dtype(series) or all(
            isinstance(v, (date, datetime, np.datetime64, pd.Timestamp))
            for v in values
        ):
            sql_type = "DATETIME2(7)"

        elif all(
            isinstance(v, numeric_types) and not isinstance(v, boolean_types)
            for v in values
        ):
            numbers = [Decimal(str(v)) for v in values]
            finite_numbers = [n for n in numbers if n.is_finite()]

            if not finite_numbers:

                sql_type = "نیازمند بررسی دستی"
            else:
                low = min(finite_numbers)
                high = max(finite_numbers)

                all_integers = all(
                    n == n.to_integral_value() for n in finite_numbers
                )

                if all_integers:

                    low_int, high_int = int(low), int(high)
                    min_value, max_value = low_int, high_int

                    if -2_147_483_648 <= low_int and high_int <= 2_147_483_647:
                        sql_type = "INT"
                    elif -9_223_372_036_854_775_808 <= low_int and high_int <= 9_223_372_036_854_775_807:
                        sql_type = "BIGINT"
                    elif max(len(str(abs(low_int))), len(str(abs(high_int)))) <= 38:
                        sql_type = "DECIMAL(38, 0)"
                    else:
                        sql_type = "نیازمند بررسی دستی"

                else:

                    min_value, max_value = str(low), str(high)

                    if any(
                        isinstance(v, (float, np.floating)) for v in values
                    ):
                        sql_type = "FLOAT"

                    else:

                        scale = max(
                            max(0, -n.as_tuple().exponent)
                            for n in finite_numbers
                        )
                        integer_digits = max(
                            max(1, n.adjusted() + 1)
                            for n in finite_numbers
                        )
                        precision = integer_digits + scale

                        sql_type = (
                            f"DECIMAL({precision}, {scale})"
                            if precision <= 38
                            else "نیازمند بررسی دستی"
                        )

        else:

            texts = [str(v) for v in values]
            longest_text = max(texts, key=utf16_units)
            max_text_length = len(longest_text)
            max_nvarchar_units = utf16_units(longest_text)

            sql_type = (
                f"NVARCHAR({max(1, max_nvarchar_units)})"
                if max_nvarchar_units <= 4000
                else "NVARCHAR(MAX)"
            )

        result.append({
            "column": column,
            "pandas_dtype": str(series.dtype),
            "sql_type": sql_type,
            "has_missing": missing_count > 0,
            "missing_count": missing_count,
            "min_value": min_value,
            "max_value": max_value,
            "max_text_length": max_text_length,
            "max_nvarchar_units": max_nvarchar_units,
        })

    return pd.DataFrame(result)