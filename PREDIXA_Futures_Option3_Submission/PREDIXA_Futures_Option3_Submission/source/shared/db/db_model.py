from sqlalchemy import Column, Integer, String, Float, Enum, TIMESTAMP, BigInteger, DateTime, Date, Boolean, UniqueConstraint, ForeignKey, Text, DECIMAL, func, JSON, SmallInteger, text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from sqlalchemy.orm import relationship

Base = declarative_base()

class FuturesMaster(Base):
    __tablename__ = 'futures_master'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(50), nullable=False)
    exchange = Column(String(50), nullable=False)
    expiry_date = Column(Date, nullable=False)
    instrument = Column(String(50), nullable=False, default='FUTURES')
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())
    last_updated = Column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now())


class FuturesCategoryMap(Base):
    __tablename__ = 'futures_category_map'

    id = Column(Integer, primary_key=True, autoincrement=True)
    futures_id = Column(Integer, ForeignKey('futures_master.id'), nullable=False)
    category = Column(Enum('NEAR', 'NEXT', 'FAR'), nullable=False)
    evaluation_date = Column(Date, nullable=False)

class Ind_StockMaster(Base):
    __tablename__ = 'ind_stock_master'
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_tick = Column(String(255), nullable=False)
    y_finance = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    index_id = Column(Integer, ForeignKey('index_master.index_id'))
    kite_symbol = Column(String(255), nullable=True)
    kite_token = Column(BigInteger, nullable=True)
    fyers_symbol = Column(String(255), nullable=True)
    is_proc = Column(Boolean, default=True)

    # Proper relationship to Index
    index = relationship("Index", back_populates="ind_stocks")

class Us_StockMaster(Base):
    __tablename__ = 'us_stock_master'
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_tick = Column(String(255), nullable=False)
    y_finance = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    index_id = Column(Integer, ForeignKey('index_master.index_id'))
    is_proc = Column(Boolean, default=True)

    # Proper relationship to Index
    index = relationship("Index", back_populates="us_stocks")

class CountryMaster(Base):
    __tablename__ = 'county_master'
    country_id = Column(Integer, primary_key=True, autoincrement=True)
    country_name = Column(String(100), nullable=False)
    status = Column(Boolean, default=True)
    flag = Column(String(500), nullable=False)

    exchanges = relationship("ExchangeMaster", back_populates="country")

class Order(Base):
    __tablename__ = 'order_master'
    order_id = Column(Integer, primary_key=True)
    trade_signal_id = Column(Integer)
    stock_tick = Column(String(200))
    stock_id = Column(Integer)
    country_id = Column(Integer)
    time_frame = Column(Integer)
    order_type = Column(String(200))
    entry_price = Column(Float)
    stoploss_price = Column(Float)
    target_price = Column(Float)
    stock_quantity = Column(BigInteger)
    purchased_cmp_date = Column(String(200))
    purchased_on = Column(DateTime)
    order_status = Column(String(200))
    is_trade_started = Column(Boolean, default=False)
    is_evaluated = Column(Boolean, default=False)
    entry_timestamp = Column(DateTime)
    completed_on = Column(DateTime)
    lstm_rf_model_prediction = Column(String(200))
    lstm_rf_model_prob = Column(Float)
    arima_ab_model_prediction = Column(String(200))
    arima_ab_model_prob = Column(Float)
    zone_signature = Column(String(255), nullable=True)
    base_start_idx = Column(Integer, nullable=True)
    legout_end_idx = Column(Integer, nullable=True)


class Auto_Order(Base):
    __tablename__ = 'auto_order_master'
    order_id = Column(Integer, primary_key=True)
    stock_tick = Column(String(200))
    stock_id = Column(Integer)
    country_id = Column(Integer)
    time_frame = Column(Integer)
    order_type = Column(String(200))
    entry_price = Column(Float)
    stoploss_price = Column(Float)
    target_price = Column(Float)
    stock_quantity = Column(BigInteger)
    purchased_cmp_date = Column(String(200))
    purchased_on = Column(DateTime)
    order_status = Column(String(200))
    is_trade_started = Column(Boolean, default=False)
    is_evaluated = Column(Boolean, default=False)
    entry_timestamp = Column(DateTime)
    completed_on = Column(DateTime)
    lstm_rf_model_prediction = Column(String(200))
    lstm_rf_model_prob = Column(Float)
    arima_ab_model_prediction = Column(String(200))
    arima_ab_model_prob = Column(Float)


class Future_Auto_Order(Base):
    __tablename__ = 'future_auto_order_master'
    order_id = Column(Integer, primary_key=True)
    trade_signal_id = Column(Integer)
    stock_tick = Column(String(200))
    stock_id = Column(Integer)
    country_id = Column(Integer)
    time_frame = Column(Integer)
    order_type = Column(String(200))
    entry_price = Column(Float)
    stoploss_price = Column(Float)
    target_price = Column(Float)
    stock_quantity = Column(BigInteger)
    purchased_cmp_date = Column(String(200))
    purchased_on = Column(DateTime)
    order_status = Column(String(200))
    is_trade_started = Column(Boolean, default=False)
    is_evaluated = Column(Boolean, default=False)
    entry_timestamp = Column(DateTime)
    completed_on = Column(DateTime)
    lstm_rf_model_prediction = Column(String(200))
    lstm_rf_model_prob = Column(Float)
    arima_ab_model_prediction = Column(String(200))
    arima_ab_model_prob = Column(Float)
    expiry_date = Column(String(50), nullable=True)


class User(Base):
    __tablename__ = 'user_master'
    id = Column(Integer, primary_key=True)
    user_name = Column(String(100))
    email = Column(String(100))
    password = Column(String(100))
    user_role = Column(String(100), default='user')
    token = Column(String(100))
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    alerts = relationship("UserAlert", back_populates="user", cascade="all, delete-orphan")
    device_tokens = relationship("UserDeviceToken", back_populates="user", cascade="all, delete-orphan")


class UserAlert(Base):
    __tablename__ = "user_alerts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("user_master.id", ondelete="CASCADE"), nullable=False)
    country_id = Column(Integer, nullable=False)
    stock_symbol = Column(String(50), nullable=False)
    exchange = Column(String(10), nullable=False)

    alert_type = Column(String(50), nullable=False)  # e.g., price_cross, price_range, etc.
    trigger_price = Column(DECIMAL(10, 2), nullable=True)
    price_range_min = Column(DECIMAL(10, 2), nullable=True)
    price_range_max = Column(DECIMAL(10, 2), nullable=True)
    threshold = Column(DECIMAL(10, 2), default=0.00)

    timeframe = Column(String(10), nullable=True)  # e.g., 1m, 5m, 1d
    note = Column(Text, nullable=True)

    is_active = Column(Boolean, default=True)
    recurrent = Column(Boolean, default=False)  
    triggered_at = Column(TIMESTAMP, nullable=True)

    # Future-scope columns
    expiry_date = Column(String(255), nullable=True)
    email_notification = Column(Boolean, default=False)
    webhook_url = Column(String(255), nullable=True)
    target_percentage = Column(DECIMAL(5, 2), nullable=True)
    cooldown_minutes = Column(Integer, default=0)

    created_at = Column(TIMESTAMP, default=datetime.now())
    updated_at = Column(TIMESTAMP, default=datetime.now(), onupdate=datetime.now())
    is_sys = Column(Boolean, default=True)
    trade_signal_id = Column(Integer)

    entry_notification_sent = Column(Boolean, default=False)
    entry_notification_sent_at = Column(DateTime)
    entry_notification_seen = Column(Boolean, default=False)
    entry_notification_seen_at = Column(DateTime)
    stoploss_target_notification_sent = Column(Boolean, default=False)
    stoploss_target_notification_sent_at = Column(DateTime)
    stoploss_target_notification_seen = Column(Boolean, default=False)
    stoploss_target_notification_seen_at = Column(DateTime)
    notification_type = Column(String)

    # Relationship to User (assumes User model exists)
    user = relationship("User", back_populates="alerts")

class UserDeviceToken(Base):
    __tablename__ = 'user_device_tokens'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('user_master.id', ondelete='CASCADE'), nullable=False)
    fcm_token = Column(String(255), nullable=False, unique=True)
    platform = Column(String(50), nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP)

    # Optional relationship to user
    user = relationship("User", back_populates="device_tokens", lazy="joined")


class CommoditiesMaster(Base):
    __tablename__ = 'commodities_master'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(50), nullable=False)
    full_name = Column(String(100))
    exchange = Column(String(50))
    unit = Column(String(50))
    category = Column(String(50))
    contract_type = Column(Enum('Main', 'Mini', 'Micro'), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    last_updated = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)
    kite_symbol = Column(String(100))
    kite_token = Column(BigInteger)


class GLOBAL_COMMODITY_ALERTS(Base):
    __tablename__ = "global_commodity_alerts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("user_master.id", ondelete="CASCADE"), nullable=False)
    country_id = Column(Integer, nullable=False)
    stock_symbol = Column(String(50), nullable=False)
    exchange = Column(String(10), nullable=False)

    alert_type = Column(String(50), nullable=False)  # e.g., price_cross, price_range, etc.
    trigger_price = Column(DECIMAL(10, 2), nullable=True)
    price_range_min = Column(DECIMAL(10, 2), nullable=True)
    price_range_max = Column(DECIMAL(10, 2), nullable=True)
    threshold = Column(DECIMAL(10, 2), default=0.00)

    timeframe = Column(String(10), nullable=True)  # e.g., 1m, 5m, 1d
    note = Column(Text, nullable=True)

    is_active = Column(Boolean, default=True)
    recurrent = Column(Boolean, default=False)
    triggered_at = Column(TIMESTAMP, nullable=True)

    # Future-scope columns
    expiry_date = Column(TIMESTAMP, nullable=True)
    email_notification = Column(Boolean, default=False)
    webhook_url = Column(String(255), nullable=True)
    target_percentage = Column(DECIMAL(5, 2), nullable=True)
    cooldown_minutes = Column(Integer, default=0)

    created_at = Column(TIMESTAMP, default=datetime.now())
    updated_at = Column(TIMESTAMP, default=datetime.now(), onupdate=datetime.now())


class TradeSignal(Base):
    __tablename__ = 'trade_signals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(Integer, nullable=False)
    stock_name = Column(String(100), nullable=False)
    time_fr = Column(Integer, nullable=False)  # 1 = Daily, 2 = 60-min, 3 = 15-min
    country_id = Column(Integer, nullable=False)
    price_cmp = Column(Float)
    trade_type = Column(String(10))            # BUY / SELL
    rrr = Column(Float)
    timestamps = Column(JSON)                  # { "timestamp_list": [...] }
    trade_data = Column(JSON)                  # { "entry_price": ..., "target_price": ..., ... }
    prediction = Column(String(20))            # e.g., "success"
    probability = Column(Float)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP)
    is_active = Column(Boolean, nullable=False)
    is_send = Column(Boolean)
    exp_date = Column(String(100), nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "exchange_id": self.exchange_id,
            "stock_name": self.stock_name,
            "time_fr": self.time_fr,
            "country_id": self.country_id,
            "price_cmp": self.price_cmp,
            "trade_type": self.trade_type,
            "rrr": self.rrr,
            "timestamps": self.timestamps,
            "trade_data": self.trade_data,
            "prediction": self.prediction,
            "probability": self.probability,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "is_active": self.is_active,
            "is_send": self.is_send,
            "exp_date": self.exp_date,
        }


class Future_Order(Base):
    __tablename__ = 'future_order_master'
    order_id = Column(Integer, primary_key=True)
    trade_signal_id = Column(Integer)
    stock_tick = Column(String(200))
    stock_id = Column(Integer)
    country_id = Column(Integer)
    time_frame = Column(Integer)
    order_type = Column(String(200))
    entry_price = Column(Float)
    stoploss_price = Column(Float)
    target_price = Column(Float)
    stock_quantity = Column(BigInteger)
    purchased_cmp_date = Column(String(200))
    purchased_on = Column(DateTime)
    order_status = Column(String(200))
    is_trade_started = Column(Boolean, default=False)
    is_evaluated = Column(Boolean, default=False)
    entry_timestamp = Column(DateTime)
    completed_on = Column(DateTime)
    lstm_rf_model_prediction = Column(String(200))
    lstm_rf_model_prob = Column(Float)
    arima_ab_model_prediction = Column(String(200))
    arima_ab_model_prob = Column(Float)
    expiry_date = Column(String(50), nullable=True)

class Commodity_Order(Base):
    __tablename__ = 'commodity_order_master'
    order_id = Column(Integer, primary_key=True)
    trade_signal_id = Column(Integer)
    stock_tick = Column(String(200))
    stock_id = Column(Integer)
    country_id = Column(Integer)
    time_frame = Column(Integer)
    order_type = Column(String(200))
    entry_price = Column(Float)
    stoploss_price = Column(Float)
    target_price = Column(Float)
    stock_quantity = Column(BigInteger)
    purchased_cmp_date = Column(String(200))
    purchased_on = Column(DateTime)
    order_status = Column(String(200))
    is_trade_started = Column(Boolean, default=False)
    is_evaluated = Column(Boolean, default=False)
    entry_timestamp = Column(DateTime)
    completed_on = Column(DateTime)
    lstm_rf_model_prediction = Column(String(200))
    lstm_rf_model_prob = Column(Float)
    arima_ab_model_prediction = Column(String(200))
    arima_ab_model_prob = Column(Float)
    expiry_date = Column(String(50), nullable=True)

class ExchangeMaster(Base):
    __tablename__ = 'exchange_master'
    exchange_id = Column(Integer, primary_key=True)
    exchange_code = Column(String)
    exchange_name = Column(String)
    country_id = Column(Integer, ForeignKey('county_master.country_id'), nullable=False)
    is_active = Column(Boolean)

    country = relationship("CountryMaster", back_populates="exchanges")

class Index(Base):
    __tablename__ = 'index_master'

    index_id = Column(Integer, primary_key=True, autoincrement=True)
    index_name = Column(String(45), nullable=False)
    y_finance = Column(String(45), nullable=True)
    is_active = Column(BigInteger, default=1)
    country_id = Column(Integer, nullable=True)

    # Correct relationship to StockMaster
    us_stocks = relationship("Us_StockMaster", back_populates="index")
    ind_stocks = relationship("Ind_StockMaster", back_populates="index")
    ind_stock_mappings = relationship("Ind_StockIndexMap", back_populates="index")
    us_stock_mappings = relationship("Us_StockIndexMap", back_populates="index")

class Us_StockIndexMap(Base):
    __tablename__ = "us_stock_index_map"

    index_map_id = Column(Integer, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey("us_stock_master.id"))
    index_id = Column(Integer, ForeignKey("index_master.index_id"))
    index = relationship("Index", back_populates="us_stock_mappings")

class Ind_StockIndexMap(Base):
    __tablename__ = "ind_stock_index_map"

    index_map_id = Column(Integer, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey("ind_stock_master.id"))
    index_id = Column(Integer, ForeignKey("index_master.index_id"))

    index = relationship("Index", back_populates="ind_stock_mappings")

class UserCountryAccess(Base):
    __tablename__ = 'user_country_access'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('user_master.id'), nullable=False)
    country_id = Column(Integer, ForeignKey('county_master.country_id'), nullable=False)
    granted_by = Column(String(100), nullable=True)
    granted_at = Column(DateTime, default=datetime.utcnow)



class IndexExchangeMap(Base):
    __tablename__ = 'index_exchange_map'

    map_id = Column(Integer, primary_key=True, autoincrement=True)
    index_id = Column(Integer, ForeignKey('index_master.index_id'), nullable=False)
    exchange_id = Column(Integer, ForeignKey('exchange_master.exchange_id'), nullable=False)
    is_primary_index = Column(Boolean, default=False)

    index = relationship("Index", lazy='joined')

class CustomDashboard(Base):
    __tablename__ = 'custom_dashboard'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer)
    country_id = Column(Integer)
    stock_id = Column(Integer)
    stock_tick = Column(String(255))
    future_symbol = Column(String(255))
    expiry_date = Column(Date)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)

class SubscriptionPlan(Base):
    __tablename__ = 'subscription_plan'

    plan_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    plan_name = Column(String(100), nullable=False)
    description = Column(Text)
    price = Column(DECIMAL(10, 2), nullable=False)
    duration_in_days = Column(Integer, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class UserSubscription(Base):
    __tablename__ = 'user_subscription'
    subscription_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('user_master.id'), nullable=False)
    plan_id = Column(Integer, ForeignKey('subscription_plan.plan_id'), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class Watchlist(Base):
    __tablename__ = 'watchlist'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('user_master.id'), nullable=False)
    ind_stock_id = Column(Integer, ForeignKey('ind_stock_master.id'), nullable=True)
    us_stock_id = Column(Integer, ForeignKey('us_stock_master.id'), nullable=True)
    country_id = Column(Integer, ForeignKey('county_master.country_id'), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", backref="watchlist_items")

class CustomAlert(Base):
    __tablename__ = 'custom_alerts'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('user_master.id'))
    country_id = Column(Integer, ForeignKey('county_master.country_id'))
    exchange_id = Column(Integer, ForeignKey('exchange_master.exchange_id'))
    stock_symbol = Column(String)
    threshold = Column(Float)
    condition = Column(Boolean)
    message = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime)
    is_active = Column(Boolean, default=True)
    notification_sent = Column(Boolean, default=False)
    notification_sent_at = Column(DateTime)
    notification_seen = Column(Boolean, default=False)
    notification_seen_at = Column(DateTime)


class OMSOrders(Base):
    __tablename__ = "oms_orders"
    pk = Column(Integer, primary_key=True, autoincrement=True)

    internal_order_id = Column(Integer, nullable=False)
    fyers_order_id = Column(String(64))
    exchange_order_id = Column(String(64))

    client_id = Column(String(45), nullable=False)
    pan = Column(String(16))

    #symbol = Column(String(64), nullable=False)
    fytoken = Column(String(64))

    segment = Column(Integer, nullable=False)
    exchange = Column(SmallInteger)
    instrument = Column(SmallInteger)

    side = Column(Integer, nullable=False)
    time_frame = Column(Integer, nullable=False)

    order_type = Column(String(45), nullable=False)
    product_type = Column(String(45), nullable=False)
    validity = Column(String(45), nullable=False)

    #qty = Column(Integer, nullable=False)
    filled_qty = Column(Integer, nullable=False, default=0)
    remaining_qty = Column(Integer, nullable=False, default=0)

    entry_price = Column(DECIMAL(18, 6))
    stoploss_price = Column(DECIMAL(18, 6))
    target_price = Column(DECIMAL(18, 6))

    status_code = Column(Integer, nullable=False)
    status_text = Column(String(45), nullable=False)
    reject_message = Column(String(512))

    offline_order = Column(Integer, nullable=False, default=0)
    is_amo = Column(Integer, nullable=False, default=0)
    is_final = Column(Integer, default=0)
    has_error = Column(Integer)

    source = Column(String(45))

    order_time_ist = Column(DateTime)

    created_at = Column(DateTime, nullable=False, default=datetime.now())
    updated_at = Column(DateTime, nullable=False, default=datetime.now())


class OMSOrderEvents(Base):
    __tablename__ = "oms_order_events"

    pk = Column(BigInteger, primary_key=True, autoincrement=True)

    internal_order_id = Column(Integer, nullable=False)
    fyers_order_id = Column(String(64))
    exchange_order_id = Column(String(64))

    client_id = Column(String(32), nullable=False)
    symbol = Column(String(64), nullable=False)
    fytoken = Column(String(64))
    segment = Column(SmallInteger, nullable=False)

    status_code = Column(SmallInteger, nullable=False)
    message = Column(String(512))

    qty = Column(Integer, nullable=False)
    filled_qty = Column(Integer, nullable=False)
    remaining_qty = Column(Integer, nullable=False)

    limit_price = Column(DECIMAL(18, 6))
    stop_price = Column(DECIMAL(18, 6))
    traded_price = Column(DECIMAL(18, 6))

    side = Column(SmallInteger, nullable=False)
    time_frame = Column(Integer, nullable=False)
    order_type = Column(SmallInteger, nullable=False)
    product_type = Column(String(16), nullable=False)
    validity = Column(String(8), nullable=False)

    offline_order = Column(Boolean, nullable=False)
    source = Column(String(32))

    order_time_ist = Column(String(32))

    raw_json = Column(JSON, nullable=False)

    received_at = Column(DateTime, nullable=False, default=datetime.now())




class OMSOrderBucket(Base):
    __tablename__ = "oms_order_bucket"

    order_id = Column(Integer)

    trade_signal_id = Column(Integer)
    stock_tick = Column(String(200))
    stock_id = Column(Integer)
    country_id = Column(Integer)

    time_frame = Column(Integer, default=1)

    order_type = Column(String(200))

    entry_price = Column(Float)
    stoploss_price = Column(Float)
    target_price = Column(Float)

    stock_quantity = Column(BigInteger)

    purchased_cmp_date = Column(String(200))
    purchased_on = Column(DateTime)

    order_status = Column(String(200))

    is_trade_started = Column(SmallInteger, default=0)
    is_evaluated = Column(SmallInteger, default=0)

    entry_timestamp = Column(DateTime)
    completed_on = Column(DateTime)

    lstm_rf_model_prediction = Column(String(45))
    lstm_rf_model_prob = Column(Float)

    arima_ab_model_prediction = Column(String(45))
    arima_ab_model_prob = Column(Float)

    approval_status = Column(String(20), default="pending")
    approved_by = Column(Integer)
    approved_at = Column(DateTime)
    id_fyers = Column(String(255))
    gtt_id = Column(String(255))
    id_fyers_gtt_oco = Column(String(255))
    gtt_oco_id = Column(String(255))

    bucket_id = Column(Integer, primary_key=True, autoincrement=True)


class TickerTape(Base):
    __tablename__ = "ticker_tape"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_tick = Column(String(45))
    expiry_date = Column(Date)
    fyers_symbol = Column(String(45))
    exchange_id = Column(Integer)
    exchange = Column(String(45))
    order = Column(Integer)
    status = Column(Boolean)


class OMSOrderBucketFuture(Base):
    __tablename__ = "oms_order_bucket_future"

    order_id = Column(Integer)

    trade_signal_id = Column(Integer)
    stock_tick = Column(String(200))
    stock_id = Column(Integer)
    country_id = Column(Integer)

    time_frame = Column(Integer, default=1)

    order_type = Column(String(200))

    entry_price = Column(Float)
    stoploss_price = Column(Float)
    target_price = Column(Float)

    stock_quantity = Column(BigInteger)

    purchased_cmp_date = Column(String(200))
    purchased_on = Column(DateTime)

    order_status = Column(String(200))

    is_trade_started = Column(SmallInteger, default=0)
    is_evaluated = Column(SmallInteger, default=0)

    entry_timestamp = Column(DateTime)
    completed_on = Column(DateTime)

    expiry_date = Column(String(45))

    lstm_rf_model_prediction = Column(String(45))
    lstm_rf_model_prob = Column(Float)

    arima_ab_model_prediction = Column(String(45))
    arima_ab_model_prob = Column(Float)

    approval_status = Column(String(20), default="pending")
    approved_by = Column(Integer)
    approved_at = Column(DateTime)
    id_fyers = Column(String(255))
    gtt_id = Column(String(255))

    bucket_id = Column(Integer, primary_key=True, autoincrement=True)


class OMSOrderBucketCommodity(Base):
    __tablename__ = "oms_order_bucket_commodity"

    order_id = Column(Integer)

    trade_signal_id = Column(Integer)
    stock_tick = Column(String(200))
    stock_id = Column(Integer)
    country_id = Column(Integer)

    time_frame = Column(Integer, default=1)

    order_type = Column(String(200))

    entry_price = Column(Float)
    stoploss_price = Column(Float)
    target_price = Column(Float)

    stock_quantity = Column(BigInteger)

    purchased_cmp_date = Column(String(200))
    purchased_on = Column(DateTime)

    order_status = Column(String(200))

    is_trade_started = Column(SmallInteger, default=0)
    is_evaluated = Column(SmallInteger, default=0)

    entry_timestamp = Column(DateTime)
    completed_on = Column(DateTime)

    expiry_date = Column(String(45))

    lstm_rf_model_prediction = Column(String(45))
    lstm_rf_model_prob = Column(Float)

    arima_ab_model_prediction = Column(String(45))
    arima_ab_model_prob = Column(Float)

    approval_status = Column(String(20), default="pending")
    approved_by = Column(Integer)
    approved_at = Column(DateTime)
    id_fyers = Column(String(255))
    gtt_id = Column(String(255))

    bucket_id = Column(Integer, primary_key=True, autoincrement=True)