import os
from scripts.stock_logic import proc_delta_stock_items
from shared.db.dbconn import DBConnection 
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from sqlalchemy import text
import pandas as pd
import csv
from pathlib import Path
from fyers_apiv3 import fyersModel
from shared.config.settings import stock_data_dir_config
from shared.utils.logger import logger
from collections import deque
from data_fetchers.fyers.fyers_session import fyers_access_token_handler
from tqdm import tqdm 
import time
import threading

class INDIAN_STOCK_DATA_UPDATER:
    def __init__(self):
        self.download_time_frames = ['D', '60', '15', '5']
        self.frame_name_dict = {
            'D': 'daily',
            '60': 'sixty',  
            '15': 'fifteen',
            '5': 'five'
        }
        full_access_token = fyers_access_token_handler().get_access_token()
        self.fyers = fyersModel.FyersModel(client_id=full_access_token.rsplit(":", 1)[0], is_async=False, token=full_access_token.rsplit(":", 1)[1], log_path="")

    def chunked(self, iterable, n):
        for i in range(0, len(iterable), n):
            yield iterable[i:i + n]

    def __getstate__(self):
        state = self.__dict__.copy()
        # Don't send heavy/unpicklable stuff to workers
        state['fyers'] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Workers don't need fyers (only main uses it).
        # If ever needed, recreate here.
        if self.fyers is None:
            pass

    def return_now_and_previous_date(self, frame: str):
        now_date = datetime.now()
        if frame == 'D' or frame == 'W' or frame == 'M':
            start_date = now_date - timedelta(days=99)
        elif frame == '60':
            start_date = now_date - timedelta(days=99)
        elif frame == '15':
            start_date = now_date - timedelta(days=99)
        elif frame == '5':
            start_date = now_date - timedelta(days=99)
        return now_date, start_date

    def dump_csv(self, header, csv_file, data):
        print("creating_csv", csv_file)
        csv_dump_path = os.path.join(stock_data_dir_config.indian_stock_data_dir,'latest_data_csv', csv_file)
        with open(csv_dump_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header)
            writer.writeheader()
            writer.writerows(data)
        print(f'CSV file "{csv_file}" created.')

    def create_monthly_and_weekly_from_daily(self, file_name: str, sample: str):
        txt_dir = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv') 
        file_path = os.path.join(txt_dir, file_name)
        df = pd.read_csv(file_path)[['open', 'high', 'low', 'close', 'qty', 'tradeDate']]
        df['timstamp'] = pd.to_datetime(df['tradeDate'], dayfirst=True)
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
        result_df = pd.DataFrame(records)
        print(result_df.columns, "#####################################", file_name)
        result_df['tradeDate'] = result_df['tradeDate'].dt.strftime("%d/%m/%Y %H:%M:%S")
        result_df = result_df[['open', 'high', 'low', 'close', 'qty', 'tradeDate']]
        mon_val = 'weekly' if sample == 'W' else 'monthly'
        out_file_name = os.path.join(txt_dir, file_name.replace('daily', mon_val))
        result_df.to_csv(out_file_name)
        #logger.info(f"Process for weekly/monthly data completed | {file_name.replace('_daily.csv', '')} | Process Id {os.getpid()}")


    def create_75munite_from_15munite(self, file_name: str):
        txt_dir = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv') 
        file_path = os.path.join(txt_dir, file_name)
        print("Reading:", file_path)
        df = pd.read_csv(file_path)[['open', 'high', 'low', 'close', 'qty', 'tradeDate']] # create_75munite_from_15munite
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
            #logger.info(f"Process for 75 minute data completed | {file_name.replace('_fifteen.csv', '')} | Process Id {os.getpid()}")
        else:
            print("No complete 75-minute chunks to save.")
    

    def create_25munite_from_5munite(self, file_name: str):
        txt_dir = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv') 
        file_path = os.path.join(txt_dir, file_name)
        print("Reading:", file_path)
        df = pd.read_csv(file_path)[['open', 'high', 'low', 'close', 'qty', 'tradeDate']] # create_75munite_from_15munite
        chunk_size = 25 // 5
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
            csv_file_name = file_name.replace("five", "twenty_five")
            self.dump_csv(headers, csv_file_name, output)
            print("Saved as:", csv_file_name)
            #logger.info(f"Process for 25 minute data completed | {file_name.replace('_five.csv', '')} | Process Id {os.getpid()}")
        else:
            print("No complete 25-minute chunks to save.")
    

    def create_125munite_from_5munite(self, file_name: str):
        txt_dir = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv') 
        file_path = os.path.join(txt_dir, file_name)
        print("Reading:", file_path)
        df = pd.read_csv(file_path)[['open', 'high', 'low', 'close', 'qty', 'tradeDate']] # create_75munite_from_15munite
        chunk_size = 125 // 5
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
            csv_file_name = file_name.replace("five", "one_twenty_five")
            self.dump_csv(headers, csv_file_name, output)
            print("Saved as:", csv_file_name)
            #logger.info(f"Process for 125 minute data completed | {file_name.replace('_five.csv', '')} | Process Id {os.getpid()}")
        else:
            print("No complete 125-minute chunks to save.")
    

    def get_stock_details(self):
        stock_symbols = []
        stock_ticks = []
        dbc = DBConnection()
        res = dbc.raw_query(text("select stock_tick, fyers_symbol from ind_stock_master where is_active=1 and fyers_symbol IS NOT NULL;"))
        print("****************************")
        print(res)
        print("****************************")
        dbc.close_engine()
        for stock in res:
            stock_symbols.append(stock.get('fyers_symbol'))
            stock_ticks.append(stock.get('stock_tick'))
        print("Stock symbols:", stock_symbols)
        print("Stock ticks:", stock_ticks)
        return stock_symbols, stock_ticks

    def get_stock_data(self, instrument_token, start_date, end_date, interval='day', max_retries=10):
        for attempt in range(max_retries):
            try:
                data = {
                    "symbol": instrument_token,
                    "resolution":interval,
                    "date_format":"1",
                    "range_from":start_date.strftime("%Y-%m-%d"),
                    "range_to":end_date.strftime("%Y-%m-%d"),
                    "cont_flag":"1"
                }
                response = self.fyers.history(data=data)
                if 'candles' in response.keys():
                    df = pd.DataFrame(response['candles'], columns=['tradeDate', 'open', 'high', 'low', 'close', 'qty'])
                    df['tradeDate'] = pd.to_datetime(df['tradeDate'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
                    # df['tradeDate'] = df['tradeDate'].dt.strftime('%d/%m/%Y %H:%M:%S')
                    df = df[['open', 'high', 'low', 'close', 'qty', 'tradeDate']]
                    return df
                else:
                    print(response, instrument_token, "Retrying .........................")
                    wait = 2 ** attempt
                    time.sleep(wait)
            except Exception as e:
                print(f"Error fetching data for token {instrument_token} {interval}: {e}")
                if '429' in str(e):
                    wait = 2 ** attempt
                    print(f"[{instrument_token}] Rate limit hit. Retrying after {wait}s...")
                    time.sleep(wait)
                else:
                    raise e
                # return pd.DataFrame()

    def format_and_store_delta_stock_data(self, df: pd.DataFrame, tick: str, interval: str):
        df = df.sort_values('tradeDate', ascending=True)

        output_dir = Path(os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv'))
        output_dir.mkdir(parents=True, exist_ok=True)
        out_file_name = output_dir / f"{tick}_{self.frame_name_dict[interval]}.csv"

        if out_file_name.exists():
            existing_data = pd.read_csv(out_file_name)
            existing_data['tradeDate'] = pd.to_datetime(existing_data['tradeDate'], format='%d/%m/%Y %H:%M:%S')
            # if file_name.__contains__('monthly') or file_name.__contains__('weekly') or file_name.__contains__('daily'):
                
            # existing_data = existing_data.drop(existing_data.index[-2])  # Drop possibly incomplete row
            # else:
                # existing_data = existing_data.drop(existing_data.index[-2])  # Drop possibly incomplete row
            # last_stamp = existing_data.iloc[-1]['tradeDate']
            # Only keep new rows
            overlap_n = 5
            start_idx = max(0, len(existing_data) - overlap_n)
            overlap_start_ts = existing_data.iloc[start_idx]['tradeDate'] if len(existing_data) > 0 else pd.Timestamp.min
            df_filtered = df[df['tradeDate'] >= overlap_start_ts].copy()
        else:
            df_filtered = df

        # Format before saving to CSV
         # create_75munite_from_15munite

        if not df_filtered.empty:
            if out_file_name.exists():
                existing_data['tradeDate'] = existing_data['tradeDate'].dt.strftime('%d/%m/%Y %H:%M:%S')
                df_filtered['tradeDate'] = df_filtered['tradeDate'].dt.strftime('%d/%m/%Y %H:%M:%S')
                df_filtered = df_filtered[['open', 'high', 'low', 'close', 'qty', 'tradeDate']]

                # base_existing = existing_data.iloc[:max(0, len(existing_data) - 5)]

                combined_data = pd.concat([existing_data, df_filtered]).drop_duplicates(subset=['tradeDate'], keep='last').reset_index(drop=True)
                combined_data.to_csv(out_file_name, index=False)
                print(f"✅ Updated {out_file_name} with {len(df_filtered)} new rows.")
            else:
                df_filtered.to_csv(out_file_name, index=False)
                print(f"✅ Created {out_file_name} with {len(df_filtered)} rows.")
        else:
            print(f"No new data to update for {tick}")


    def all_downloader_kite(self):
        try:
            stock_symbols, stock_ticks = self.get_stock_details()
            request_times = deque()
            total_req = len(stock_symbols) * len(self.download_time_frames)
            ctr = 0
            for interval in self.download_time_frames:
                end_date, start_date = self.return_now_and_previous_date(interval)
                for idx, stock_name in enumerate(stock_symbols):
                    try:
                        now = time.time()
                        request_times.append(now)
                        while request_times and now - request_times[0] > 60:
                            request_times.popleft()

                        if len(request_times) > 190:
                            print("⏳ Rate limit nearing (minute). Sleeping 5s...")
                            time.sleep(5)

                        if len(request_times) >= 10:
                            time_since_first = now - request_times[0]
                            if time_since_first < 1:
                                time.sleep(1 - time_since_first)

                        stock_data = self.get_stock_data(stock_name, start_date, end_date, interval)
                        # print(stock_data['tradeDate'].head())
                        if not stock_data.empty:
                            self.format_and_store_delta_stock_data(stock_data, stock_ticks[idx], interval)
                            
                            # if interval == '5':
                            #     filename = f"{stock_ticks[idx]}_five.csv"
                            #     #self.create_25munite_from_5munite(filename)
                            #     #self.create_125munite_from_5munite(filename)
                                
                                
                        else:
                            print(f"No data for token {stock_name} {interval}")
                        ctr += 1
                        logger.info(f"[{ctr}/{total_req}] Downloaded {stock_name} {interval}")
                    except Exception as ex:
                        logger.error(f"Error fetching/storing {stock_name} {interval}: {ex}", exc_info=True)

        except Exception as ex:
            logger.exception(ex, stack_info=True)
            raise StopIteration

    def _build_timeframes_for_tick(self, tick: str):
        base_dir = stock_data_dir_config.indian_stock_data_dir
        txt_dir = os.path.join(base_dir, 'latest_data_csv')

        daily_file = f"{tick}_daily.csv"
        fifteen_file = f"{tick}_fifteen.csv"
        five_file = f"{tick}_five.csv"

        # Guard reads: skip if file missing
        daily_path = os.path.join(txt_dir, daily_file)
        if os.path.exists(daily_path):
            self.create_monthly_and_weekly_from_daily(daily_file, 'W')
            self.create_monthly_and_weekly_from_daily(daily_file, 'ME')

        fifteen_path = os.path.join(txt_dir, fifteen_file)
        if os.path.exists(fifteen_path):
            self.create_75munite_from_15munite(fifteen_file)

        five_path = os.path.join(txt_dir, five_file)
        if os.path.exists(five_path):
            self.create_25munite_from_5munite(five_file)
            self.create_125munite_from_5munite(five_file)

    def process_all(self):
        try:
            input_dir = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv')
            output_dir = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'processed_data_files')
            
            self.all_downloader_kite()
            
            stock_symbols, stock_ticks = self.get_stock_details()

            # os.makedirs(input_dir, exist_ok=True)
            # os.makedirs(output_dir, exist_ok=True)
            workers = max(1, (os.cpu_count() or 2) - 1)
            # timeframe_futures = []

            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(self._build_timeframes_for_tick, tick): tick
                    for tick in stock_ticks
                }

                for f in tqdm(as_completed(futures),
                            total=len(futures),
                            desc="Building derived timeframes",
                            unit="symbol",
                            dynamic_ncols=True):
                    tick = futures[f]
                    try:
                        f.result()
                    except Exception as e:
                        logger.error("Timeframe generation failed for %s: %s", tick, e, exc_info=True)

            items = list(os.listdir(input_dir)) 
            
            # with ProcessPoolExecutor(max_workers=workers) as tp_pool:
            #     delta_futures = [
            #         tp_pool.submit(proc_delta_stock_items, stock_item, input_dir, output_dir)
            #         for stock_item in items
            #     ]

            #     for f in tqdm(
            #         as_completed(delta_futures),
            #         total=len(delta_futures),
            #         desc="Processing stocks",
            #         unit="file",
            #         dynamic_ncols=True
            #     ):
            #         try:
            #             f.result()
            #         except Exception as e:
            #             logger.exception("Error processing a stock file: %s", e)

            print("Indian stock update process complete.")
            logger.info("Indian stock update process complete.")
        except Exception as ex:
            logger.error("Fatal error in process_all", exc_info=True)
            print("Indian stock update process failed. See logs for details.")

        print("Indian stock update process complete ...........................")

    # def process_all(self):
    #     try:
    #         input_dir = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv')
    #         output_dir = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'processed_data_files')
    #         self.all_downloader_kite()
    #         all_file_path = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv')
    #         thread_count = 0
    #         # for file in os.listdir(all_file_path):
    #         #     if 'daily' in file:
    #         #         print(file, "********************************")
    #         #         # self.create_monthly_and_weekly_from_daily(file, 'W')
    #         #         # self.create_monthly_and_weekly_from_daily(file, 'ME')
                    
    #         #         thread_weekly = threading.Thread(target = self.create_monthly_and_weekly_from_daily, args = (file, 'W',))
    #         #         thread_weekly.start()
    #         #         thread_count += 1
    #         #         logger.info(f"Thread for weekly data started | {file.replace('_daily.csv','')} | Thread No {thread_count}")
                    
    #         #         thread_monthly = threading.Thread(target = self.create_monthly_and_weekly_from_daily, args = (file, 'ME',))
    #         #         thread_monthly.start()
    #         #         thread_count += 1
    #         #         logger.info(f"Thread for monthly data started | {file.replace('_daily.csv','')} | Thread No {thread_count}")

    #         #     if 'fifteen' in file:
    #         #         #self.create_75munite_from_15munite(file)
    #         #         thread_75 = threading.Thread(target = self.create_75munite_from_15munite, args = (file,))
    #         #         thread_75.start()
    #         #         thread_count += 1
    #         #         logger.info(f"Thread for 75 minute data started | {file.replace('_fifteen.csv','')} | Thread No {thread_count}")
            
    #         stock_symbols, stock_ticks = self.get_stock_details()

    #         # daily_file_names = [f"{stock_ticks[idx]}_daily.csv" for idx, stock_name in enumerate(stock_symbols)]
    #         # print(daily_file_names)
    #         # with ProcessPoolExecutor(max_workers=3) as exe:
    #         #     exe.map(self.create_monthly_and_weekly_from_daily, daily_file_names, ['W'] * len(stock_symbols))
    #         # with ProcessPoolExecutor(max_workers=3) as exe1:
    #         #     exe1.map(self.create_monthly_and_weekly_from_daily, daily_file_names, ['ME'] * len(stock_symbols))
            
    #         # fifteen_file_names = [f"{stock_ticks[idx]}_fifteen.csv" for idx, stock_name in enumerate(stock_symbols)]
    #         # print(fifteen_file_names)
    #         # with ProcessPoolExecutor(max_workers=3) as exe2:
    #         #     exe2.map(self.create_75munite_from_15munite, fifteen_file_names)
            
    #         # five_file_names = [f"{stock_ticks[idx]}_five.csv" for idx, stock_name in enumerate(stock_symbols)]
    #         # print(five_file_names)
    #         # with ProcessPoolExecutor(max_workers=3) as exe3:
    #         #     exe3.map(self.create_25munite_from_5munite, five_file_names)
    #         # with ProcessPoolExecutor(max_workers=3) as exe4:
    #         #     exe4.map(self.create_125munite_from_5munite, five_file_names)
            
            
    #         for idx, stock_name in enumerate(stock_symbols):

    #             #########################################################################################
    #             # processes for data created from daily data --------------------------------------------
    #             filename = f"{stock_ticks[idx]}_daily.csv"

    #             thread_weekly = threading.Thread(target = self.create_monthly_and_weekly_from_daily, args = (filename, 'W',))
    #             thread_weekly.start()
    #             thread_count += 1
    #             logger.info(f"Thread for weekly data started | {stock_ticks[idx]} | Thread No {thread_count}")
                
    #             thread_monthly = threading.Thread(target = self.create_monthly_and_weekly_from_daily, args = (filename, 'ME',))
    #             thread_monthly.start()
    #             thread_count += 1
    #             logger.info(f"Thread for monthly data started | {stock_ticks[idx]} | Thread No {thread_count}")
                
                
    #             #############################################################################################
    #             # processes for data created from 15 minute data --------------------------------------------
    #             filename = f"{stock_ticks[idx]}_fifteen.csv"

    #             thread_75 = threading.Thread(target = self.create_75munite_from_15munite, args = (filename,))
    #             thread_75.start()
    #             thread_count += 1
    #             logger.info(f"Thread for 75 minute data started | {stock_ticks[idx]} | Thread No {thread_count}")


    #             #################################################################################
    #             # processes for data created from 5 minute data ---------------------------------
    #             filename = f"{stock_ticks[idx]}_five.csv"
                  
    #             thread_25 = threading.Thread(target=self.create_25munite_from_5munite, args=(filename,))
    #             thread_25.start()
    #             thread_count += 1
    #             logger.info(f"Thread for 25 minute data started | {stock_ticks[idx]} | Thread No {thread_count}")
            
    #             thread_125 = threading.Thread(target=self.create_125munite_from_5munite, args=(filename,))
    #             thread_125.start()
    #             thread_count += 1
    #             logger.info(f"Thread for 125 minute data started | {stock_ticks[idx]} | Thread No {thread_count}")
            
                
    #         items = list(os.listdir(input_dir))
    #         workers = max(1, (os.cpu_count() or 2) - 1)

    #         with ProcessPoolExecutor(max_workers=workers) as executor:
    #             futures = [executor.submit(proc_delta_stock_items, stock_item, input_dir, output_dir) for stock_item in items]
    #             for f in tqdm(as_completed(futures), total=len(futures), desc="Processing stocks", unit="file", dynamic_ncols=True):
    #               try:
    #                 _ = f.result()  # surface exceptions, if any
    #               except Exception as e:
    #                 logger.exception("Error processing a stock file: %s", e)
                    
    #         print("Indian stock update process complete.")
    #     except Exception as ex:
    #         logger.error(ex, stack_info=True, exc_info=True)

    #     print("Indian stock update process complete ...........................")
