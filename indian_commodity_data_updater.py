import pandas as pd
from datetime import datetime
# from apps.finprod_backend.futures.nsep import start_date
from shared.config.settings import stock_data_dir_config as sddcfg
from shared.db.dbconn import DBConnection
from shared.db.db_model import CommoditiesMaster
from sqlitedict import SqliteDict
from concurrent.futures import ProcessPoolExecutor, as_completed
from scripts.stock_logic import proc_delta_stock_items
import os, re, csv
from datetime import datetime, timedelta
from data_fetchers.fyers.fyers_session import fyers_access_token_handler
from fyers_apiv3 import fyersModel
import time
import requests
from io import StringIO
from tqdm import tqdm

class MCXFutureDataHandler:
    def __init__(self):
        full_access_token = fyers_access_token_handler().get_access_token()
        self.fyers = fyersModel.FyersModel(client_id=full_access_token.rsplit(":", 1)[0], is_async=False, token=full_access_token.rsplit(":", 1)[1], log_path="")
        self.download_path = os.path.join(sddcfg.indian_commodity_data, 'latest_data_csv')
        self.output_path = os.path.join(sddcfg.indian_commodity_data, 'processed_data_files')
        self.commodities = self.get_commodity_symbols()
        self.lite_db = SqliteDict('finprod.sqlite')
        self.name_interval_dict = {
            "D" : "daily", 
            "60": "sixty", 
            "15": "fifteen",
            "120": "one_twenty",
            "240": "two_forty",
        }
        self.update_latest_3_expiries_mcx()
    
    def get_commodity_symbols(self):
        dbc = DBConnection()
        session = dbc.get_session()
        symbols = session.query(CommoditiesMaster.symbol).filter(CommoditiesMaster.is_active == 1).all()
        sym_list = [s[0] for s in symbols]
        dbc.close_engine()
        return sym_list

    def get_latest_fyers_commodity_symbol_expiry(self, product: str):
        try:
            # Step 1: Download the latest file
            url = "https://public.fyers.in/sym_details/MCX_COM.csv"
            response = requests.get(url)
            response.raise_for_status()  # Raise error if request failed
            # Step 2: Read into DataFrame (no headers in file)
            df = pd.read_csv(StringIO(response.text), header=None)
            # Step 3: Filter for product
            df = df[df[9].astype(str).str.contains(f"MCX:{product}", na=False)]
            df = df[df[9].astype(str).str.contains("FUT", na=False)]
            # Step 4: Convert expiry column (index 10) to datetime
            df["parsed_date"] = df[1].apply(self.extract_date)
            df = df.drop_duplicates(subset=["parsed_date"], keep='last').reset_index(drop=True)
            df = df.dropna(subset=["parsed_date"]).sort_values(by="parsed_date", ascending=True)
            df['parsed_date'] = df['parsed_date'].dt.strftime('%d-%m-%Y')
            # Step 5: Sort and return latest symbol
            print(df.tail()['parsed_date'])
            fdf = pd.DataFrame({
                "symbol": df[9].to_list()[:3],
                "expiry": df["parsed_date"].to_list()[:3]
            })
            # Step 5: Sort and return latest symbol
            return fdf.to_dict('records')

        except Exception as e:
            print(f"Error fetching latest symbol: {e}")
            return None

    def extract_date(self, entry):
        match = re.search(r'\b(\d{2})\s([A-Za-z]{3})\s(\d{2})\b', entry)
        if match:
            # year, month, day = match.groups()
            day, month, year = match.groups()
            full_date_str = f"20{year} {month} {day}"
            return datetime.strptime(full_date_str, "%Y %b %d")
        return None

    def create_monthly_and_weekly_from_daily(self, file_name: str, sample: str):
        txt_dir = os.path.join(sddcfg.indian_commodity_data, 'latest_data_csv') 
        file_path = os.path.join(txt_dir, file_name)
        df = pd.read_csv(file_path)[['open', 'high', 'low', 'close', 'qty', 'tradeDate']]
        df['timstamp'] = pd.to_datetime(df['tradeDate'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['timstamp']).sort_values('timstamp').reset_index(drop=True)
        df.set_index('timstamp', inplace=True)
        # df = df.sort_values(by='tradeDate', ascending=True)
        # Resample to weekly frequency
        re_sample = 'W-FRI' if sample == 'W' else sample
        grouped = df.groupby(pd.Grouper(freq=re_sample))
        records = []
        for _, group in grouped:
            if group.empty:
                continue
            first_date = group.index[0]
            open_ = group['open'].iloc[0]
            high_ = group['high'].max()
            low_ = group['low'].min()
            close_ = group['close'].iloc[-1]
            qty_ = group['qty'].sum()

            records.append({
                "tradeDate": first_date,
                "open": open_,
                "high": high_,
                "low": low_,
                "close": close_,
                "qty": qty_
            })
        # print(records)
        result_df = pd.DataFrame(records)
        # print(result_df.columns, "#####################################", file_name)
        result_df['tradeDate'] = result_df['tradeDate'].dt.strftime("%d/%m/%Y %H:%M:%S")
        result_df = result_df[['open', 'high', 'low', 'close', 'qty', 'tradeDate']]
        mon_val = 'weekly' if sample == 'W' else 'monthly'
        out_file_name = os.path.join(txt_dir, file_name.replace('daily', mon_val))
        result_df.to_csv(out_file_name)
    
    def return_now_and_previous_date(self, frame: str):
        now_date = datetime.now()
        start_date = now_date - timedelta(days=365)
        # if frame == 'D' or frame == 'M' or frame == 'W':
        #     start_date = now_date - timedelta(days=365)
        # elif frame == '60':
        #     start_date = now_date - timedelta(days=100)
        # elif frame == '15':
        #     start_date = now_date - timedelta(days=100)
        # elif frame == '120':
        #     start_date = now_date - timedelta(days=100)
        # elif frame == '240':
        #     start_date = now_date - timedelta(days=100)
        return now_date, start_date


    def create_75munite_from_15munite(self, file_name: str):
        txt_dir = os.path.join(sddcfg.indian_commodity_data, 'latest_data_csv') 
        file_path = os.path.join(txt_dir, file_name)
        print("Reading:", file_path)
        df = pd.read_csv(file_path)[['open', 'high', 'low', 'close', 'qty', 'tradeDate']] # create_75munite_from_15munite
        df['_dt'] = pd.to_datetime(df['tradeDate'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['_dt']).sort_values('_dt').reset_index(drop=True)
        df = df[['open', 'high', 'low', 'close', 'qty', 'tradeDate']]
        chunk_size = 5
        output = []
        # Define operation map
        agg_map = {
            'open': lambda chunk: chunk.iloc[0]['open'],
            'high': lambda chunk: chunk['high'].max(),
            'low': lambda chunk: chunk['low'].min(),
            'close': lambda chunk: chunk.iloc[-1]['close'],
            'qty': lambda chunk: chunk['qty'].sum(),
            # 'tradeTime': lambda chunk: chunk.iloc[0]['tradeTime'],
            'tradeDate': lambda chunk: chunk.iloc[0]['tradeDate']
        }
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            if len(chunk) < chunk_size:
                break  # Skip incomplete chunk

            new_row = {col: func(chunk) for col, func in agg_map.items()}
            output.append(new_row)

        if output:
            headers = output[0].keys()
            csv_file_name = file_name.replace("fifteen", "seventy_five")
            self.dump_csv(headers, csv_file_name, output)
            print("Saved as:", csv_file_name)
        else:
            print("No complete 75-minute chunks to save.")


    
    def dump_csv(self, header, csv_file, data):
        print("creating_csv", csv_file)
        csv_dump_path = os.path.join(sddcfg.indian_commodity_data,'latest_data_csv', csv_file)
        with open(csv_dump_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header)
            writer.writeheader()
            writer.writerows(data)
        print(f'CSV file "{csv_file}" created.')

    def _max_days_for_interval(self, interval: str) -> int:
        if interval in ("D", "1D", "W", "M"):
            return 366
        return 100

    def _iter_date_chunks(self, start_date: datetime, end_date: datetime, chunk_days: int):
        current = start_date
        while current <= end_date:
            chunk_to = min(current + timedelta(days=chunk_days - 1), end_date)
            yield current, chunk_to
            current = chunk_to + timedelta(days=1)

    def update_latest_3_expiries_mcx(self):
        out_res = []
        for symbol in self.commodities:
            int_dict = {}
            int_dict['symbol'] = symbol
            contracts = self.get_latest_fyers_commodity_symbol_expiry(symbol)
            expiry_list = [item["expiry"] for item in contracts]
            int_dict['expiry'] = expiry_list
            out_res.append(int_dict)
            
        self.lite_db['indian_commodity_expiries'] = out_res
        self.lite_db.commit()
        self.lite_db.close()

    def fetch_candle_data(self, instrument_token, from_date, to_date, interval, max_retries=10):
        chunk_days = self._max_days_for_interval(interval)
        dfs = []

        for chunk_from, chunk_to in self._iter_date_chunks(from_date, to_date, chunk_days):
            df_chunk = self._fetch_candle_data_once(
                instrument_token,
                chunk_from,
                chunk_to,
                interval,
                max_retries
            )

            if df_chunk is not None and not df_chunk.empty:
                dfs.append(df_chunk)

        if not dfs:
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'qty', 'tradeDate'])

        df = pd.concat(dfs, ignore_index=True)

        # remove duplicates (edge overlaps)
        df.drop_duplicates(subset=['tradeDate'], keep='last', inplace=True)

        # sort properly
        df['_dt'] = pd.to_datetime(df['tradeDate'], dayfirst=True, errors='coerce')
        df.sort_values('_dt', inplace=True)
        df.drop(columns=['_dt'], inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df

    def _fetch_candle_data_once(self, instrument_token, from_date, to_date, interval, max_retries=10):
        for attempt in range(max_retries):
            try:
                data = {
                        "symbol": instrument_token,
                        "resolution":interval,
                        "date_format":"1",
                        "range_from":from_date.strftime("%Y-%m-%d"),
                        "range_to":to_date.strftime("%Y-%m-%d"),
                        "cont_flag":"1"
                    }

                response = self.fyers.history(data=data)
                if 'candles' in response.keys():
                    df = pd.DataFrame(response['candles'], columns=['tradeDate', 'open', 'high', 'low', 'close', 'qty'])
                    df['tradeDate'] = pd.to_datetime(df['tradeDate'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
                    df['tradeDate'] = df['tradeDate'].dt.strftime('%d/%m/%Y %H:%M:%S')
                    df = df[['open', 'high', 'low', 'close', 'qty', 'tradeDate']]
                    return df
                else:
                    print(response, instrument_token, "Retrying .........................")
                    wait = 2 ** attempt
                    time.sleep(wait)
            except Exception as e:
                print(f"Error fetching for token {instrument_token}: {e}")
                return pd.DataFrame()

    def download_all(self, intervals=['D', '60', '15', '120', '240']):
        # today = datetime.now()    
        for symbol in self.commodities:
            contracts = self.get_latest_fyers_commodity_symbol_expiry(symbol)
            for items in contracts:
                future_token = items['symbol']
                expiry = items['expiry']
                numbers = re.findall(r'\d+', expiry)
                # Join all found numbers into one string
                exp_num = ''.join(numbers)
                for interval in intervals:
                    # end_dt, start_dt = self.return_now_and_previous_date(interval)
                    # df = self.fetch_candle_data(future_token, start_dt, end_dt, interval)
                    # if not df.empty:
                    fname = f"{symbol}_{exp_num}_{self.name_interval_dict[interval]}.csv"
                    file_path = os.path.join(self.download_path, fname)
                    if os.path.exists(file_path):
                        try:
                            existing_df = pd.read_csv(file_path)
                            if not existing_df.empty:
                                last_date = pd.to_datetime(existing_df['tradeDate'], dayfirst=True, errors='coerce').max()
                                # fetch only recent data (small window instead of full history)
                                if pd.isna(last_date):
                                    delta_start = datetime.now() - timedelta(days=365)
                                else:
                                    delta_start = last_date - timedelta(days=5)

                                delta_end = datetime.now()
                                df_delta = self._fetch_candle_data_once(future_token, delta_start, delta_end, interval)
                                if df_delta is not None and not df_delta.empty:
                                    combined = pd.concat([existing_df, df_delta], ignore_index=True)
                                    # remove duplicates
                                    combined.drop_duplicates(subset=['tradeDate'], keep='last', inplace=True)
                                    # sort
                                    combined['_dt'] = pd.to_datetime(combined['tradeDate'], dayfirst=True, errors='coerce')
                                    combined.sort_values('_dt', inplace=True)
                                    combined.drop(columns=['_dt'], inplace=True)
                                    combined.reset_index(drop=True, inplace=True)

                                    combined.to_csv(file_path, index=False)
                                    print(f"🔄 Updated (delta): {fname}")
                                else:
                                    print(f"⚠️ No delta data: {fname}")

                        except Exception as exx:
                            print(f"❌ Error updating {fname}: {exx}")

                        # df.to_csv(os.path.join(self.download_path, fname), index=False)
                        # print(f"Saved: {fname}")
                    else:
                        
                        end_dt, start_dt = self.return_now_and_previous_date(interval)
                        df_full = self.fetch_candle_data(future_token, start_dt, end_dt, interval)
                        if df_full is not None and not df_full.empty:
                            df_full.to_csv(file_path, index=False)
                            print(f"✅ Created (full): {fname}")
                        else:
                            print(f"⚠️ No full data: {symbol} {interval}")

                        print("skipping empty data", symbol, interval)

        all_file_list = os.listdir(self.download_path)
        for items in all_file_list:
            if str(items).__contains__("fifteen"):
                self.create_75munite_from_15munite(items)
            elif str(items).__contains__("daily"):
                self.create_monthly_and_weekly_from_daily(items, 'W')
                self.create_monthly_and_weekly_from_daily(items, 'ME')
    
    # def process_all(self):
    #     with ThreadPoolExecutor(max_workers=10) as executor:
    #         for stock_item in os.listdir(self.download_path):
    #             executor.submit(proc_delta_stock_items, stock_item, self.download_path, self.output_path)
    #     print("all_stock_process_complete...................")
    def process_all(self):
        all_files = os.listdir(self.download_path)

        with ProcessPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(
                    proc_delta_stock_items,
                    stock_item,
                    self.download_path,
                    self.output_path
                )
                for stock_item in all_files
            ]

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Processing commodities"
            ):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error while processing: {e}")

        print("all_stock_process_complete...................")
        executor.shutdown(wait=False)
    

    def create_125_munite_from_5_munite(self, file_name: str):
        txt_dir = "path/to/directory"
        file_path = os.path.join(txt_dir, file_name)
        print("Reading:", file_path)

        df = pd.read_csv(file_path)[['open', 'high', 'low', 'close', 'qty', 'tradeDate']]
        
        chunk_size = 125 // 5
        output = []

        # Define operation map
        agg_map = {
            'open': lambda chunk: chunk.iloc[0]['open'],
            'high': lambda chunk: chunk['high'].max(),
            'low': lambda chunk: chunk['low'].min(),
            'close': lambda chunk: chunk.iloc[-1]['close'],
            'qty': lambda chunk: chunk['qty'].sum(),
            'tradeDate': lambda chunk: chunk.iloc[0]['tradeDate']
        }

        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            if len(chunk) < chunk_size:
                break  # Skip incomplete chunk

            new_row = {col: func(chunk) for col, func in agg_map.items()}
            output.append(new_row)

        if output:
            headers = output[0].keys()
            csv_file_name = file_name.replace("five", "one_twenty_five")
            self.dump_csv(headers, csv_file_name, output)
            print("Saved as:", csv_file_name)
        else:
            print("No complete 125-minute chunks to save.")




