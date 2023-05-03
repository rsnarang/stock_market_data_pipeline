import glob
import json
import os
import gc
import zipfile
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dagster import asset, graph
from pathlib import Path
from time import time

import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split

# Edit this on other systems
path = Path(r"D:\projects\stock_market_data_pipeline\data")


def download_kaggle_dataset() -> None:
    os.system('kaggle datasets download -d jacksoncrow/stock-market-dataset')


def extract_kaggle_dataset(path: Path) -> None:
    with zipfile.ZipFile(f"{path}/stock-market-dataset.zip", "r") as zip_file:
        zip_file.extractall()


def get_kaggle_data() -> None:
    global path
    path = Path(f"{path}")
    path.mkdir(parents=True, exist_ok=True)
    download_kaggle_dataset()
    extract_kaggle_dataset(path)


# We're ready to go

def get_file_list(path: str) -> list:
    file_list = glob.glob(f"{path}/*.csv")
    return file_list


def load_file(path):
    return pd.read_csv(f"{path}", engine='pyarrow')


def get_symbol(path: str) -> str:
    symbol = os.path.basename(path)
    symbol = symbol.split('.csv')[0]
    return symbol


# Multithreading due to being I/O bound
def create_file_df_dict() -> dict:
    global path
    stock_files = get_file_list(f"{path}/stocks")
    etf_files = get_file_list(f"{path}/etfs")
    file_df_dict = {}
    with ThreadPoolExecutor() as executor:
        stock_futures = {executor.submit(load_file, file): file for file in stock_files}
        etf_futures = {executor.submit(load_file, file): file for file in etf_files}

        for future in as_completed(stock_futures):
            symbol = get_symbol(stock_futures[future])
            file_df_dict[symbol] = future.result()
        #
        for future in as_completed(etf_futures):
            symbol = get_symbol(etf_futures[future])
            file_df_dict[symbol] = future.result()

    return file_df_dict


def make_symbol_dict() -> None:
    global path
    meta_path = f"{path}/symbols_valid_meta.csv"
    df = pd.read_csv(meta_path, engine='pyarrow')
    symbol_dict = df.set_index('NASDAQ Symbol')['Security Name'].to_dict()
    with open(f"{path}/symbol_dict.json", "w") as file_path:
        json.dump(symbol_dict, file_path)


def get_symbol_name_dict() -> dict:
    global path
    meta_path = f"{path}"
    with open(f"{path}/symbol_dict.json", "r") as file_path:
        symbol_dict = json.load(file_path)

    return symbol_dict


def transform_each_df(symbol_df_tuple: tuple) -> pd.DataFrame:
    symbol, df = symbol_df_tuple
    symbol_name_dict = get_symbol_name_dict()
    df['Symbol'] = symbol
    df['Security Name'] = symbol_name_dict[symbol]
    return df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    symbol_col = df.pop("Symbol")
    security_col = df.pop("Security Name")
    df.insert(1, "Symbol", symbol_col)
    df.insert(2, "Security", security_col)
    return df


# Parallel
def transform_all_dfs(file_df_dict: dict) -> None:
    make_symbol_dict()
    symbol_df_tuple_list = list(file_df_dict.items())
    with ProcessPoolExecutor() as pool:
        df_list = pool.map(transform_each_df, symbol_df_tuple_list)
    transformed_df = pd.concat(df_list, axis=0)
    save_transformed_df(transformed_df)


def downcast_df(df: pd.DataFrame) -> pd.DataFrame:
    fcols = df.select_dtypes('float').columns
    icols = df.select_dtypes('integer').columns

    df[fcols] = df[fcols].apply(pd.to_numeric, downcast='float')
    df[icols] = df[icols].apply(pd.to_numeric, downcast='integer')

    return df


def save_transformed_df(transformed_df: pd.DataFrame) -> None:
    global path
    work_path = Path(f"{path}/transformed")
    work_path.mkdir(parents=True, exist_ok=True)
    transformed_df = downcast_df(transformed_df)
    transformed_df.reset_index(drop=True, inplace=True)
    transformed_df['Date'] = pd.to_datetime(transformed_df['Date']).values
    transformed_df.dropna(inplace=True)
    transformed_df = transformed_df.astype({'Symbol': 'string',
                                            'Security Name': 'string',
                                            'Volume': 'int32'})
    transformed_df.sort_values(by=['Symbol', 'Date'], inplace=True)
    pd.DataFrame.to_parquet(transformed_df, f"{work_path}/transformed_df.parquet")


def get_transformed_df() -> pd.DataFrame:
    global path
    work_path = Path(f"{path}/transformed/transformed_df.parquet")
    df = pd.read_parquet(str(work_path))
    return df


def moving_average(df: pd.DataFrame) -> pd.DataFrame:
    df['vol_moving_avg'] = df.groupby("Symbol")["Volume"].rolling(window=30). \
        mean().reset_index(0, drop=True)
    df = df.astype({'vol_moving_avg': 'float32'})
    return df


def adj_close_rolling_median(df: pd.DataFrame) -> pd.DataFrame:
    df['adj_close_rolling_med'] = df.groupby("Symbol")["Close"].rolling(window=30). \
        mean().reset_index(0, drop=True)
    df = df.astype({'adj_close_rolling_med': 'float32'})
    return df


def eng_features(df: pd.DataFrame) -> pd.DataFrame:
    global path
    work_path = Path(f"{path}/transformed/final")
    work_path.mkdir(parents=True, exist_ok=True)
    df = moving_average(df)
    df = adj_close_rolling_median(df)
    df.set_index('Date', inplace=True)
    df.dropna(inplace=True)
    df.to_parquet(f"{work_path}/final_df.parquet")
    return df


def get_final_df() -> pd.DataFrame:
    global path
    work_path = Path(f"{path}/transformed/final")
    df = pd.read_parquet(f"{work_path}/final_df.parquet")
    return df


def train_ml_model():
    global path
    work_path = Path(f"{path}/transformed/final")
    df = get_final_df()

    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'

    X = df[features]
    y = df[target]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    xgb_model = xgb.XGBRegressor(n_jobs=-1, tree_method='gpu_hist', objective='reg:squarederror')
    xgb_model.fit(X_train, y_train)

    Path.mkdir(work_path, parents=True, exist_ok=True)
    xgb_model.save_model(f"{work_path}/xgb.model")

    # Make predictions on test data
    y_pred = xgb_model.predict(X_test)

    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    return (y_pred, mae, mse)

@graph
def main():
    start = time()
    df_dict = create_file_df_dict()
    gc.collect()
    transform_all_dfs(df_dict)
    pd.set_option('display.max_columns', None)
    gc.collect()
    df = get_transformed_df()
    df = eng_features(df)
    train_ml_model()
    end = time()

