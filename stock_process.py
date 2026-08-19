from datetime import datetime, time
from sqlitedict import SqliteDict
import dagster as dg
from data_fetchers.fyers.fyers_session import fyers_access_token_handler
from data_fetchers.fyers.indian_stock_data_updater import INDIAN_STOCK_DATA_UPDATER
from data_fetchers.yfinance.us_stock_data_updater import US_STOCK_DATA_UPDATER
from data_fetchers.fyers.indian_commodity_data_updater import MCXFutureDataHandler
from data_fetchers.fyers.indian_futures_data_updater import Indian_future_data_handler, future_data_processer
from apps.pipelines.trade_accumulator_cash import daily_trades_accumulate, sixty_trades_accumulate, fifteen_trades_accumulate, seventy_five_trades_accumulate
from apps.pipelines.trade_accumulator_cash import one_twenty_five_trades_accumulate, twenty_five_trades_accumulate
from apps.pipelines.trade_accumulator_future_commodity import daily_trades_accumulate_futures_and_commodity, sixty_trades_accumulate_futures_and_commodity, fifteen_trades_accumulate_futures_and_commodity, one_twenty_trades_accumulate_futures_and_commodity, two_forty_trades_accumulate_futures_and_commodity
# from alert_engine.alert import start_entry_alert_engine, setup_alert_engine
from apps.pipelines.miscellaneous_jobs import delete_duplicate_trade_signals, delete_completed_trade_signals, update_order_status_functions, update_futures_expiry, delete_expired_futures_commodity_data
from dagster import job, op, DefaultScheduleStatus, Failure
from apps.pipelines.stock_split_handler import STOCK_SPLIT_CHECKER
# from apps.pipelines.market_news import save_news
from apps.pipelines.rag_finance_firecrawl import save_news
from scripts.scanner import SetupScannerOrchestrator, ScanJob
from scripts.scanner_fc import SetupScannerOrchestrator_FC, ScanJob_FC, build_futures_expiry_map, build_commodity_expiry_map
from shared.config.settings import stock_logic_config, validate_required_config
from scripts.cash_wetrun_scanner import run_cash_wetrun_scanner


validate_required_config()

TABLE_NAME = {"india": "ind_stock_master", "us": "us_stock_master"}

################################# OPS ###############################################################

db = SqliteDict("finprod.sqlite")

@op(config_schema={"country": str})
def accumulate_daily_trades(context):
    country = context.op_config["country"]
    print(country, "country")
    # daily_trades_accumulate(country)

    orchestrator = SetupScannerOrchestrator()
    job = ScanJob(
        time_lists=[stock_logic_config.TIME_FRAME_EXE_1],
        time_frame=1,
        last_d_time=datetime.now(),
        output_path="scan_results_daily.json",
        append=True
    )
    orchestrator.run(job)

@op
def accumulate_daily_trades_futures(context):
    # daily_trades_accumulate_futures_and_commodity('futures')
    orchestrator = SetupScannerOrchestrator_FC()
    job = ScanJob_FC(
        time_lists=[stock_logic_config.FUTURE_TIME_FRAME_1],
        time_frame=1,
        last_d_time=datetime.now(),
        output_path="scan_results_future_daily.json",
        append=True,
        is_future=True
    )
    orchestrator.run(job)

@op
def accumulate_daily_trades_commodity(context):
    #daily_trades_accumulate_futures_and_commodity('commodity')
    orchestrator = SetupScannerOrchestrator_FC()
    job = ScanJob_FC(
        time_lists=[stock_logic_config.COMMODITY_TIME_FRAME_1],
        time_frame=1,
        last_d_time=datetime.now(),
        output_path="scan_results_commodity_daily.json",
        append=True,
        is_future=False
    )
    orchestrator.run(job)

@op
def run_operation_sixty_trades_futures(context):
    # sixty_trades_accumulate_futures_and_commodity('futures')
    orchestrator = SetupScannerOrchestrator_FC()
    job = ScanJob_FC(
        time_lists=[stock_logic_config.FUTURE_TIME_FRAME_2],
        time_frame=2,
        last_d_time=datetime.now(),
        output_path="scan_results_future_sixty.json",
        append=True,
        is_future=True
    )
    orchestrator.run(job)

@op
def run_operation_sixty_trades_commodity(context):
    # sixty_trades_accumulate_futures_and_commodity('commodity')
    orchestrator = SetupScannerOrchestrator_FC()
    job = ScanJob_FC(
        time_lists=[stock_logic_config.COMMODITY_TIME_FRAME_4],
        time_frame=4,
        last_d_time=datetime.now(),
        output_path="scan_results_commodity_sixty.json",
        append=True,
        is_future=False
    )
    orchestrator.run(job)

@op
def run_operation_one_twenty_trades_commodity(context):
    # one_twenty_trades_accumulate_futures_and_commodity('commodity')
    orchestrator = SetupScannerOrchestrator_FC()
    job = ScanJob_FC(
        time_lists=[stock_logic_config.COMMODITY_TIME_FRAME_3],
        time_frame=3,
        last_d_time=datetime.now(),
        output_path="scan_results_commodity_one_twenty.json",
        append=True,
        is_future=False
    )
    orchestrator.run(job)

@op
def run_operation_two_forty_trades_commodity(context):
    # two_forty_trades_accumulate_futures_and_commodity('commodity')
    orchestrator = SetupScannerOrchestrator_FC()
    job = ScanJob_FC(
        time_lists=[stock_logic_config.COMMODITY_TIME_FRAME_2],
        time_frame=2,
        last_d_time=datetime.now(),
        output_path="scan_results_commodity_two_forty.json",
        append=True,
        is_future=False
    )
    orchestrator.run(job)

@op(config_schema={"country": str})
def accumulate_sixty_trades(context):
    country = context.op_config["country"]
    print(country, "country")
    # sixty_trades_accumulate(country)

    orchestrator = SetupScannerOrchestrator()
    job = ScanJob(
        time_lists=[stock_logic_config.TIME_FRAME_EXE_2],
        time_frame=2,
        last_d_time=datetime.now(),
        output_path="scan_results_sixty.json",
        append=True
    )
    orchestrator.run(job)

@op(config_schema={"country": str})
def accumulate_fifteen_trades(context):
    country = context.op_config["country"]
    print(country, "country")
    # fifteen_trades_accumulate(country)
    orchestrator = SetupScannerOrchestrator()
    job = ScanJob(
        time_lists=[stock_logic_config.TIME_FRAME_EXE_3],
        time_frame=3,
        last_d_time=datetime.now(),
        output_path="scan_results_fifteen.json",
        append=True
    )
    orchestrator.run(job)

@op(config_schema={"country": str})
def accumulate_seventy_five_trades(context):
    country = context.op_config["country"]
    print(country, "country")
    # seventy_five_trades_accumulate(country)
    orchestrator = SetupScannerOrchestrator()
    job = ScanJob(
        time_lists=[stock_logic_config.TIME_FRAME_EXE_25],
        time_frame=25,
        last_d_time=datetime.now(),
        output_path="scan_results_seventy_five.json",
        append=True
    )
    orchestrator.run(job)

@op
def fetch_us_stock_data(context):
    ussdu = US_STOCK_DATA_UPDATER()
    ussdu.process_all()

@op
def fetch_india_stock_data(context):
    isdu = INDIAN_STOCK_DATA_UPDATER()
    isdu.process_all()

@op
def future_data_india(context):
    ifdh = Indian_future_data_handler()
    fdp = future_data_processer()
    ifdh.process()
    fdp.process_all_files()

@op
def indian_commodity_data(context):
    mcx_handler = MCXFutureDataHandler()
    mcx_handler.download_all()
    mcx_handler.process_all()
    return "Completed"


@op
def run_alert_engine_in_market_hours():
    # Define market hours (IST)
    start_time = time(9, 15)
    end_time = time(15, 30)

    # Check current time
    now = datetime.now().time()

    if not (start_time <= now <= end_time):
        print("⏹️ Outside market hours, exiting early.")
        return

    print("✅ Inside market hours, starting engine...")
    # start_entry_alert_engine()

    # Run until market closes
    while True:
        now = datetime.now().time()
        if now >= end_time:
            print("🔚 Market closed. Stopping engine.")
            break
        time.sleep(30)  # Check every 30 seconds

@op
def update_fyers_token():
    fath = fyers_access_token_handler()
    fath.main()


@op
def delete_duplicate_trade_signals_op():
    delete_duplicate_trade_signals()

@op
def delete_completed_trade_signals_op():
    delete_completed_trade_signals()

@op
def update_order_status_op():
    update_order_status_functions()


@op
def check_stock_splits_op():
    checker = STOCK_SPLIT_CHECKER()
    checker.calculate()


@op
def market_news_op():
    save_news()


@op(config_schema={"country": str})
def accumulate_one_twenty_five_trades(context):
    country = context.op_config["country"]
    print(country, "country")
    # one_twenty_five_trades_accumulate(country)
    orchestrator = SetupScannerOrchestrator()
    job = ScanJob(
        time_lists=[stock_logic_config.TIME_FRAME_EXE_5],
        time_frame=5,
        last_d_time=datetime.now(),
        output_path="scan_results_one_twenty_five.json",
        append=True
    )
    orchestrator.run(job)


@op(config_schema={"country": str})
def accumulate_twenty_five_trades(context):
    country = context.op_config["country"]
    print(country, "country")
    # twenty_five_trades_accumulate(country)
    orchestrator = SetupScannerOrchestrator()
    job = ScanJob(
        time_lists=[stock_logic_config.TIME_FRAME_EXE_6],
        time_frame=6,
        last_d_time=datetime.now(),
        output_path="scan_results_twenty_five.json",
        append=True
    )
    orchestrator.run(job)

@op()
def update_futures_expiry_op():
    update_futures_expiry()


# @op
# def delete_invalid_trade_signals_op():
#     res = delete_invalid_trade_signals()

@op
def delete_expired_futures_commodity_data_op():
    delete_expired_futures_commodity_data()

@op
def cash_wetrun_scanner_op():
    run_cash_wetrun_scanner(
            output_path="cash_wetrun_scan_results.jsonl",
            append=True,
        )

######################################################################################################################
################################################### JOBS #############################################################

@job
def update_fyers_token_job():
    update_fyers_token()


@job
def accumulate_daily_trades_job_india():
    accumulate_daily_trades()

@job
def accumulate_daily_trades_job_futures():
    accumulate_daily_trades_futures()
    # accumulate_daily_trades_futures

@job
def accumulate_daily_trades_job_commodity():
    accumulate_daily_trades_commodity()

@job
def run_job_sixty_trades_job_futures():
    run_operation_sixty_trades_futures()
    # accumulate_daily_trades_futures

@job
def run_job_sixty_trades_job_commodity():
    run_operation_sixty_trades_commodity()

@job
def run_job_one_twenty_trades_job_commodity():
    run_operation_one_twenty_trades_commodity()

@job
def run_job_two_forty_trades_job_commodity():
    run_operation_two_forty_trades_commodity()

@job
def accumulate_sixty_trades_job_india():
    # Define the Indian trading hours (from 9:15 AM to 3:15 PM)
    # start_time = time(9, 14)  # 9:15 AM
    # end_time = time(15, 36)   # 3:30 PM
    # current_time = datetime.now().time()
    # # Schedule task every hour during trading hours if current time is within trading hours
    # if (start_time <= current_time <= end_time):
    accumulate_sixty_trades()

@job
def accumulate_fifteen_trades_job_india():
    # Define the Indian trading hours (from 9:15 AM to 3:15 PM)
    # start_time = time(9, 14)  # 9:15 AM
    # end_time = time(15, 36)   # 3:30 PM
    # current_time = datetime.now().time()
    # # Schedule task every 15 mins during trading hours if current time is within trading hours
    # if (start_time <= current_time <= end_time):
    #     print('starting 15 min trades.........')
    accumulate_fifteen_trades()

@job
def accumulate_seventy_five_trades_job_india():
    accumulate_seventy_five_trades()

@job
def accumulate_daily_trades_job_us():
    accumulate_daily_trades()

@job
def accumulate_sixty_trades_job_us():
    start_time = time(18, 59)  # 7:00 PM
    end_time = time(1, 36)   # 1:30 AM
    current_time = datetime.now().time()
    # Schedule task every hour during trading hours if current time is within trading hours
    if (start_time <= current_time <= end_time):
        accumulate_sixty_trades()

@job
def accumulate_fifteen_trades_job_us():
    start_time = time(18, 59)  # 7:00 PM
    end_time = time(1, 36)   # 1:30 AM
    current_time = datetime.now().time()
    # Schedule task every 15 mins during trading hours if current time is within trading hours
    if (start_time <= current_time <= end_time):
        accumulate_fifteen_trades()

@job
def update_indian_stock_data_job():
    # Define the Indian trading hours (from 9:15 AM to 3:15 PM)
    # start_time = time(9, 14)  # 9:15 AM
    # end_time = time(15, 36)   # 3:30 PM
    # current_time = datetime.now().time()
    # # Schedule task every 15 mins during trading hours if current time is within trading hours
    # if (start_time <= current_time <= end_time):
        
    print("Updating Indian Stocks data..............")
    fetch_india_stock_data()

@job
def update_us_stock_data_job():
    start_time = time(18, 59)  # 7:00 PM
    end_time = time(1, 36)   # 1:30 AM
    current_time = datetime.now().time()
    # Schedule task every 15 mins during trading hours if current time is within trading hours
    if (start_time <= current_time <= end_time):
        print("Updating US Stocks data..............")
        fetch_us_stock_data()

@job
def future_data_india_job():
    day = datetime.now().strftime('%A')
    if day != 'Saturday' and day != 'Sunday':
        print("Updating Future data for India..............")
        future_data_india()

@job
def update_indian_commodity_job():
    indian_commodity_data()

@job
def market_controlled_alert_job():
    run_alert_engine_in_market_hours()

@job
def delete_duplicate_trade_signals_job():
    delete_duplicate_trade_signals_op()

@job
def delete_completed_trade_signals_job():
    delete_completed_trade_signals_op()

@job
def update_order_status_job():
    update_order_status_op()

@job
def check_stock_splits_job():
    check_stock_splits_op()

@job
def market_news_job():
    market_news_op()

@job
def accumulate_one_twenty_five_trades_job_india():
    accumulate_one_twenty_five_trades()

@job
def accumulate_twenty_five_trades_job_india():
    accumulate_twenty_five_trades()

@job
def update_futures_expiry_job():
    update_futures_expiry_op()


# @job
# def delete_invalid_trade_signals_job():
#     delete_invalid_trade_signals_op()

@job
def delete_expired_futures_commodity_data_job():
    delete_expired_futures_commodity_data_op()

@job
def cash_wetrun_scanner_job():
    cash_wetrun_scanner_op()


################################################### SCHEDULES ###############################################################

daily_trade_schedule_india = dg.ScheduleDefinition(
    job=accumulate_daily_trades_job_india,
    cron_schedule="45 15 * * 1-5",
    name="accumulate_daily_trades_india",
    run_config={"ops": {"accumulate_daily_trades": {"config": {"country": "india"}}}},
    execution_timezone="Asia/Kolkata",
    # default_status=DefaultScheduleStatus.RUNNING
    default_status=DefaultScheduleStatus.STOPPED
)
sixty_trade_schedule_india = dg.ScheduleDefinition(
    job=accumulate_sixty_trades_job_india,
    cron_schedule="0 9-16 * * 1-5",
    name="accumulate_sixty_trades_india",
    run_config={"ops": {"accumulate_sixty_trades": {"config": {"country": "india"}}}},
    execution_timezone="Asia/Kolkata",
    # default_status=DefaultScheduleStatus.RUNNING
    default_status=DefaultScheduleStatus.STOPPED
)
fifteen_trade_schedule_india = dg.ScheduleDefinition(
    job=accumulate_fifteen_trades_job_india,
    cron_schedule="0 9-16 * * 1-5",
    name="accumulate_fifteen_trades_india",
    run_config={"ops": {"accumulate_fifteen_trades": {"config": {"country": "india"}}}},
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED
)
seventy_five_trade_schedule_india = dg.ScheduleDefinition(
    job=accumulate_seventy_five_trades_job_india,
    cron_schedule="0 9-16 * * 1-5",
    name="accumulate_seventy_five_trades_india",
    run_config={"ops": {"accumulate_seventy_five_trades": {"config": {"country": "india"}}}},
    execution_timezone="Asia/Kolkata",
    # default_status=DefaultScheduleStatus.RUNNING
    default_status=DefaultScheduleStatus.STOPPED
)
daily_trade_schedule_us = dg.ScheduleDefinition(
    job=accumulate_daily_trades_job_us,
    cron_schedule="45 1 * * 1-5",
    name="accumulate_daily_trades_us",
    run_config={"ops": {"accumulate_daily_trades": {"config": {"country": "us"}}}},
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED
)
sixty_trade_schedule_us = dg.ScheduleDefinition(
    job=accumulate_sixty_trades_job_us,
    cron_schedule="0 19-1 * * 1-5",
    name="accumulate_sixty_trades_us",
    run_config={"ops": {"accumulate_sixty_trades": {"config": {"country": "us"}}}},
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED
)
fifteen_trade_schedule_us = dg.ScheduleDefinition(
    job=accumulate_fifteen_trades_job_us,
    cron_schedule="0 19-1 * * 1-5",
    name="accumulate_fifteen_trades_us",
    run_config={"ops": {"accumulate_fifteen_trades": {"config": {"country": "us"}}}},
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED
)

indian_stock_data_updater_schedule = dg.ScheduleDefinition(
    job=update_indian_stock_data_job,
    cron_schedule="30 9-16 * * 1-5", # "*/30 9-16 * * *"
    name="update_indian_data_every_15min",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.RUNNING
)

indian_commodity_data_updater_schedule = dg.ScheduleDefinition(
    job=update_indian_commodity_job,
    cron_schedule="0/30 9-16 * * 1-5", ####"*/30 * * * *" 
    name="update_indian_commodity_data_every_15min",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.RUNNING
)

us_stock_data_updater_schedule = dg.ScheduleDefinition(
    job=update_us_stock_data_job,
    cron_schedule="0 * * * *",
    name="update_us_data_every_15min",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED
)

india_future_data_schedule = dg.ScheduleDefinition(
    job=future_data_india_job,
    cron_schedule="0/30 9-15 * * 1-5",
    name="future_data_india",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.RUNNING
)

market_start_schedule = dg.ScheduleDefinition(
    job=market_controlled_alert_job,
    cron_schedule="15 9 * * 1-5",  # 9:15 AM on weekdays
    name="start_market_alert_job",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.RUNNING
)

update_fyers_token_schedule = dg.ScheduleDefinition(
    job=update_fyers_token_job,
    cron_schedule="0 9 * * *",
    name="update_fyers_token",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.RUNNING
)

daily_trades_job_commodity_schedule = dg.ScheduleDefinition(
    job=accumulate_daily_trades_job_commodity,
    cron_schedule="45 15 * * 1-5",
    name="daily_trades_job_commodity",
    execution_timezone="Asia/Kolkata",
    # default_status=DefaultScheduleStatus.RUNNING
    default_status=DefaultScheduleStatus.STOPPED
)

daily_trades_job_futures_schedule = dg.ScheduleDefinition(
    job=accumulate_daily_trades_job_futures,
    cron_schedule="45 15 * * 1-5",
    name="daily_trades_job_futures",
    execution_timezone="Asia/Kolkata",
    # default_status=DefaultScheduleStatus.RUNNING
    default_status=DefaultScheduleStatus.STOPPED
)

sixty_trades_job_commodity_schedule = dg.ScheduleDefinition(
    job=run_job_sixty_trades_job_commodity,
    cron_schedule="0 9-16 * * *",
    name="sixty_trades_job_commodity",
    execution_timezone="Asia/Kolkata",
    # default_status=DefaultScheduleStatus.RUNNING
    default_status=DefaultScheduleStatus.STOPPED
)

sixty_trades_job_futures_schedule = dg.ScheduleDefinition(
    job=run_job_sixty_trades_job_futures,
    cron_schedule="0 9-16 * * *",
    name="sixty_trades_job_futures",
    execution_timezone="Asia/Kolkata",
    # default_status=DefaultScheduleStatus.RUNNING
    default_status=DefaultScheduleStatus.STOPPED
)

one_twenty_trades_job_commodity_schedule = dg.ScheduleDefinition(
    job=run_job_one_twenty_trades_job_commodity,
    cron_schedule="0 9-16 * * *",
    name="one_twenty_trades_job_commodity",
    execution_timezone="Asia/Kolkata",
    # default_status=DefaultScheduleStatus.RUNNING
    default_status=DefaultScheduleStatus.STOPPED
)

two_forty_trades_job_commodity_schedule = dg.ScheduleDefinition(
    job=run_job_two_forty_trades_job_commodity,
    cron_schedule="0 9-16 * * *",
    name="two_forty_trades_job_commodity",
    execution_timezone="Asia/Kolkata",
    # default_status=DefaultScheduleStatus.RUNNING
    default_status=DefaultScheduleStatus.STOPPED
)

delete_duplicate_trade_signals_schedule = dg.ScheduleDefinition(
    job=delete_duplicate_trade_signals_job,
    cron_schedule="0/30 * * * *",
    name="delete_duplicate_trade_signals",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED
)

delete_completed_trade_signals_schedule = dg.ScheduleDefinition(
    job=delete_completed_trade_signals_job,
    cron_schedule="0/30 * * * *",
    name="delete_completed_trade_signals",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED
)

update_order_status_schedule = dg.ScheduleDefinition(
    job=update_order_status_job,
    cron_schedule="0/5 9-15 * * 1-5",
    name="update_order_status",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.RUNNING
)

stock_split_schedule = dg.ScheduleDefinition(
    job=check_stock_splits_job,
    cron_schedule="0 9 * * *",
    name="check_stock_splits",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED
)

market_news_schedule = dg.ScheduleDefinition(
    job=market_news_job,
    cron_schedule="0 10 * * *",
    name="save_market_news",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED
)

one_twenty_five_trade_schedule_india = dg.ScheduleDefinition(
    job=accumulate_one_twenty_five_trades_job_india,
    cron_schedule="45 15 * * 1-5",
    name="accumulate_one_twenty_five_trades_india",
    run_config={"ops": {"accumulate_one_twenty_five_trades": {"config": {"country": "india"}}}},
    execution_timezone="Asia/Kolkata",
    # default_status=DefaultScheduleStatus.RUNNING
    default_status=DefaultScheduleStatus.STOPPED
)

twenty_five_trade_schedule_india = dg.ScheduleDefinition(
    job=accumulate_twenty_five_trades_job_india,
    cron_schedule="45 15 * * 1-5",
    name="accumulate_twenty_five_trades_india",
    run_config={"ops": {"accumulate_twenty_five_trades": {"config": {"country": "india"}}}},
    execution_timezone="Asia/Kolkata",
    # default_status=DefaultScheduleStatus.RUNNING
    default_status=DefaultScheduleStatus.STOPPED
)

update_futures_expiry_schedule = dg.ScheduleDefinition(
    job=update_futures_expiry_job,
    cron_schedule="20 9 * * 3",
    name="update_futures_expiry_job",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.RUNNING
)


# delete_invalid_trade_signals_schedule = dg.ScheduleDefinition(
#     job=delete_invalid_trade_signals_job,
#     cron_schedule="0/30 * * * *",
#     name="delete_invalid_trade_signals",
#     execution_timezone="Asia/Kolkata",
#     default_status=DefaultScheduleStatus.STOPPED
# )


delete_expired_futures_commodity_data_schedule = dg.ScheduleDefinition(
    job=delete_expired_futures_commodity_data_job,
    cron_schedule="0 10 * * 3",
    name="delete_expired_futures_commodity_data",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED
)


cash_wetrun_scanner_schedule = dg.ScheduleDefinition(
    job=cash_wetrun_scanner_job,
    cron_schedule="0 16 * * 1-5",
    name="run_cash_wetrun_scanner",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.RUNNING
)

# Assemble all definitions for Dagster
defs = dg.Definitions(
    jobs=[
            accumulate_daily_trades_job_india,
            accumulate_sixty_trades_job_india,
            accumulate_fifteen_trades_job_india,
            accumulate_seventy_five_trades_job_india,
            #accumulate_daily_trades_job_us,
            #accumulate_sixty_trades_job_us,
            #accumulate_fifteen_trades_job_us,
            #update_us_stock_data_job,
            update_indian_stock_data_job,
            future_data_india_job, 
            update_indian_commodity_job,
            #market_controlled_alert_job,
            accumulate_daily_trades_job_futures,
            accumulate_daily_trades_job_commodity,
            run_job_sixty_trades_job_futures,
            run_job_sixty_trades_job_commodity,
            run_job_one_twenty_trades_job_commodity,
            run_job_two_forty_trades_job_commodity,
            update_fyers_token_job,
            delete_duplicate_trade_signals_job,
            delete_completed_trade_signals_job,
            update_order_status_job,
            check_stock_splits_job,
            market_news_job,
            accumulate_one_twenty_five_trades_job_india,
            accumulate_twenty_five_trades_job_india,
            update_futures_expiry_job,
            #delete_invalid_trade_signals_job,
            delete_expired_futures_commodity_data_job,
            cash_wetrun_scanner_job
        ],
    schedules=[
                daily_trade_schedule_india,
                sixty_trade_schedule_india,
                fifteen_trade_schedule_india,
                seventy_five_trade_schedule_india,
                #daily_trade_schedule_us,
                #sixty_trade_schedule_us,
                #fifteen_trade_schedule_us,
                #us_stock_data_updater_schedule,
                indian_stock_data_updater_schedule,
                india_future_data_schedule, 
                indian_commodity_data_updater_schedule,
                #market_start_schedule,
                update_fyers_token_schedule,
                daily_trades_job_commodity_schedule,
                daily_trades_job_futures_schedule,
                sixty_trades_job_commodity_schedule,
                sixty_trades_job_futures_schedule,
                one_twenty_trades_job_commodity_schedule,
                two_forty_trades_job_commodity_schedule,
                delete_duplicate_trade_signals_schedule,
                delete_completed_trade_signals_schedule,
                update_order_status_schedule,
                stock_split_schedule,
                market_news_schedule,
                one_twenty_five_trade_schedule_india,
                twenty_five_trade_schedule_india,
                update_futures_expiry_schedule,
                #delete_invalid_trade_signals_schedule,
                delete_expired_futures_commodity_data_schedule,
                cash_wetrun_scanner_schedule
            ]
)