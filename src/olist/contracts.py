import hashlib
import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation

SCHEMA_VERSION = 1
SOURCE = "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
HEADERS = {
    "orders": "order_id customer_id order_status order_purchase_timestamp order_approved_at order_delivered_carrier_date order_delivered_customer_date order_estimated_delivery_date".split(),
    "customers": "customer_id customer_unique_id customer_zip_code_prefix customer_city customer_state".split(),
    "order_items": "order_id order_item_id product_id seller_id shipping_limit_date price freight_value".split(),
    "sellers": "seller_id seller_zip_code_prefix seller_city seller_state".split(),
    "products": "product_id product_category_name product_name_lenght product_description_lenght product_photos_qty product_weight_g product_length_cm product_height_cm product_width_cm".split(),
    "order_payments": "order_id payment_sequential payment_type payment_installments payment_value".split(),
    "order_reviews": "review_id order_id review_score review_comment_title review_comment_message review_creation_date review_answer_timestamp".split(),
    "geolocation": "geolocation_zip_code_prefix geolocation_lat geolocation_lng geolocation_city geolocation_state".split(),
    "category_translation": "product_category_name product_category_name_english".split(),
}
FILES = {t: f"olist_{t}_dataset.csv" for t in HEADERS}
FILES["category_translation"] = "product_category_name_translation.csv"
KEYS = {"orders": ["order_id"], "customers": ["customer_id"], "products": ["product_id"], "sellers": ["seller_id"], "order_items": ["order_id", "order_item_id"], "order_payments": ["order_id", "payment_sequential"], "order_reviews": ["review_id", "order_id"], "category_translation": ["product_category_name"]}
MONEY = {"price", "freight_value", "payment_value"}
INTEGERS = {"order_item_id", "payment_sequential", "payment_installments", "review_score", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"}
EVENTS = {"order_created": "order_purchase_timestamp", "order_approved": "order_approved_at", "handed_to_carrier": "order_delivered_carrier_date", "order_delivered": "order_delivered_customer_date"}
RANK = {name: i for i, name in enumerate(EVENTS)}
STATUSES = {"created", "approved", "invoiced", "processing", "shipped", "delivered", "unavailable", "canceled"}

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def source_utc(value):
    # Explicit fixed-offset teaching assumption; source values are retained verbatim.
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone(timedelta(hours=-3))).astimezone(timezone.utc)

def validate_event(event):
    required = ["event_id", "schema_version", "dataset_version", "replay_run_id", "order_id", "event_type", "source_event_time", "event_time", "emitted_at", "is_simulated"]
    if not isinstance(event, dict) or any(k not in event or event[k] is None for k in required):
        return "missing_fields"
    if type(event["schema_version"]) is not int or event["schema_version"] != SCHEMA_VERSION or event["is_simulated"] is not True:
        return "invalid_contract"
    if not isinstance(event["event_type"], str) or event["event_type"] not in EVENTS or not all(isinstance(event[k], str) and event[k] for k in ["event_id", "dataset_version", "replay_run_id", "order_id"]):
        return "invalid_identity"
    try:
        source_utc(event["source_event_time"])
        for k in ["event_time", "emitted_at"]:
            if datetime.fromisoformat(event[k]).tzinfo is None:
                return "timezone_required"
    except (ValueError, TypeError):
        return "invalid_timestamp"
    expected = digest([event["dataset_version"], event["replay_run_id"], event["order_id"], event["event_type"], event["source_event_time"]])
    return None if event["event_id"] == expected else "invalid_event_id"

def money(value):
    try:
        x = Decimal(value)
        if not x.is_finite() or x < 0 or x != x.quantize(Decimal("0.01")) or x >= Decimal("10000000000000000"):
            raise ValueError("invalid_money")
        return x
    except (InvalidOperation, TypeError):
        raise ValueError("invalid_money") from None
