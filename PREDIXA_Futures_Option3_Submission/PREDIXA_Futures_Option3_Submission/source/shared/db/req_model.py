from pydantic import BaseModel, Field
from typing import Optional, List, Any
from datetime import datetime, date


class UserAlertCreate(BaseModel):
    user_id: int
    country_id: int
    stock_symbol: str
    exchange: str
    alert_type: str

    trigger_price: Optional[float] = None
    price_range_min: Optional[float] = None
    price_range_max: Optional[float] = None
    threshold: Optional[float] = 0.0

    timeframe: Optional[str] = None
    note: Optional[str] = None
    is_active: Optional[bool] = True
    recurrent: Optional[bool] = False

    expiry_date: Optional[datetime] = None
    email_notification: Optional[bool] = False
    webhook_url: Optional[str] = None
    target_percentage: Optional[float] = None
    cooldown_minutes: Optional[int] = 0


class UserAlertUpdate(BaseModel):
    stock_symbol: Optional[str] = None
    exchange: Optional[str] = None
    alert_type: Optional[str] = None

    trigger_price: Optional[float] = None
    price_range_min: Optional[float] = None
    price_range_max: Optional[float] = None
    threshold: Optional[float] = None

    timeframe: Optional[str] = None
    note: Optional[str] = None
    is_active: Optional[bool] = None
    recurrent: Optional[bool] = None
    expiry_date: Optional[datetime] = None
    email_notification: Optional[bool] = None
    webhook_url: Optional[str] = None
    target_percentage: Optional[float] = None
    cooldown_minutes: Optional[int] = None

class TokenPayload(BaseModel):
    token: str
    platform: Optional[str] = None

class GetAlert(BaseModel):
    user_id: int = "All"
    country_id: int = 1
    search_key: Optional[str] = None
    start_date: str = "2025-01-01"
    end_date: str = "2025-12-31"
    page_no: Optional[str] = None
    limit: Optional[int] = 10


class OrderBase(BaseModel):
    country_id: int
    stock_id: str
    stock_tick: str
    time_frame: int
    order_type: str
    entry_price: float
    stoploss_price: float
    target_price: float
    stock_quantity: int
    purchased_cmp_date: str
    prediction:  Optional[str] = None
    probability:  Optional[float] = None
    trade_id: Optional[int] = None

class BucketOrderBase(BaseModel):
    order_id: Optional[int] = None
    country_id: int
    stock_id: str
    stock_tick: str
    time_frame: int
    order_type: str
    entry_price: float
    stoploss_price: float
    target_price: float
    stock_quantity: int
    purchased_cmp_date: str
    prediction:  Optional[str] = None
    probability:  Optional[float] = None
    trade_id: Optional[int] = None
    expiry_date: Optional[str] = None

class Update_BucketOrderBase(BaseModel):
    bucket_id: int
    order_id: Optional[int] = None
    country_id: int
    stock_id: str
    stock_tick: str
    time_frame: int
    order_type: str
    entry_price: float
    stoploss_price: float
    target_price: float
    stock_quantity: int
    purchased_cmp_date: str
    prediction:  Optional[str] = None
    probability:  Optional[float] = None
    trade_id: Optional[int] = None

class ApproveReject_BucketOrderBase(BaseModel):
    bucket_id: int
    approved_by: str
    status: str


class EditFutureCandidateRequest(BaseModel):
    """Editable prices for a never-submitted/rejected futures candidate."""
    bucket_id: int
    entry_price: float
    stoploss_price: float
    target_price: float

class Future_and_commodity_OrderBase(BaseModel):
    country_id: int
    stock_id: str
    stock_tick: str
    time_frame: int
    order_type: str
    entry_price: float
    stoploss_price: float
    target_price: float
    stock_quantity: int
    purchased_cmp_date: str
    exp_date: str
    prediction: Optional[str] = None
    probability: Optional[float] = None
    trade_id: Optional[int] = None

class GetOrder(BaseModel):
    country_id: int = 1
    stock_tick: Optional[str] = None
    start_date: str = "2025-01-01"
    end_date: str = "2025-12-31"
    status: str = "success"
    time_frame: Optional[Any] = None
    page_no: Optional[str] = None
    limit: Optional[int] = 10
    order_by_purchased_cmp_date: Optional[bool] = False
    order_type: Optional[str] = "all"
    trade_id: Optional[Any] = None
    prediction_type: Optional[str] = None

class CreateStockModel(BaseModel):
    country: str
    stock_name: str
    # y_stock_name: str
    symbol: str
    index_ids: List[int]


class Stock_RCM(BaseModel):
    order_type : str
    entry_price : float
    target_price : float
    stoploss_price : float
    last_d_time : str
    time_frame: int
    country_id : int
    tick: str

class Stock_RCM_New(BaseModel):
    order_type : str
    entry_price : float
    target_price : float
    stoploss_price : float
    last_d_time : str
    time_frame: int
    mod_frame : str
    tick: str

class GetAlertNew(BaseModel):
    country_id: int = 1
    user_id: int = None
    stock_tick: Optional[str] = None
    start_date: str = "2025-01-01"
    end_date: str = "2025-12-31"
    page_no: Optional[str] = None
    limit: Optional[int] = 10
    is_system: Optional[bool] = True

class SaveDashboard(BaseModel):
    id: Optional[int] = None
    user_id: int
    country_id: int
    stock_id: Optional[int] = None
    stock_tick: Optional[str] = None
    future_symbol: Optional[str] = None
    expiry_date: Optional[str] = None

class UserSubscriptionRequest(BaseModel):
    user_id: int = Field(..., description="ID of the user")
    plan_id: int = Field(..., description="ID of the subscription plan")
    start_date: date = Field(..., description="Subscription start date")
    end_date: date = Field(..., description="Subscription end date")


class UserCountryAccessRequest(BaseModel):
    user_id: int = Field(..., description="User ID to grant access")
    country_ids: List[int] = Field(..., description="List of country IDs to grant access to")
    granted_by: str = Field(..., description="Who granted the access")

class user_cred(BaseModel):
    user_name : str
    password : str

class user_create(BaseModel):
    user_name : str
    password : str
    email: str

class WatchlistCreate(BaseModel):
    user_id: int
    stock_id: int
    country : int

class CreateIndexModel(BaseModel):
    country_id: int
    index_name: str
    symbol: str
    exchange_ids: List[int]

class GetStockModel(BaseModel):
    country_name: Optional[str] = "india"
    index_id: Optional[Any] = None
    stock_tick: Optional[str] = None
    page_no: Optional[Any] = None
    limit: Optional[int] = 10

class CustomAlertCreate(BaseModel):
    user_id: int
    country_id: int
    stock_symbol: str
    exchange_id: Optional[int] = None
    threshold: float
    condition: bool
    message: Optional[str] = None

class CustomAlertUpdate(BaseModel):
    alert_id: int
    threshold: Optional[float] = None
    condition: Optional[bool] = None
    message: Optional[str] = None

class CustomAlertGet(BaseModel):
    country_id: int = 1
    user_id: int = None
    stock_symbol: Optional[str] = None
    start_date: str = "2025-01-01"
    end_date: str = "2025-12-31"
    page_no: Optional[str] = None
    limit: Optional[int] = 10

class AddTickerTape(BaseModel):
    id: Optional[int] = None
    stock_tick: str
    expity_date: Optional[str] = "YYYY-MM-DD"
    fyers_symbol: str
    exchange_id: int
    exchange: str
    order: int

