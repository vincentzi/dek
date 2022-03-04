from tabulate import tabulate
import pandas as pd


def pretty_print_df(df: pd.DataFrame):
    print(f"\n{tabulate(df, headers='keys')}")
