# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # POS Synthetic Data Generator
# MAGIC
# MAGIC Generates **20,000 synthetic POS transactions** spanning ~92 days and writes them
# MAGIC as line-delimited JSON (NDJSON) files to a Unity Catalog Volume for Bronze ingestion.
# MAGIC
# MAGIC **Quick-start:**
# MAGIC 1. Attach to a cluster (or use serverless)
# MAGIC 2. Run All — runtime ~3-5 min for 20k transactions
# MAGIC
# MAGIC **Bundle parameters** (passed via `base_parameters` in pipeline.yml job):
# MAGIC - `catalog`           — Unity Catalog name (default: `pos_demo`)
# MAGIC - `schema`            — Schema name (default: `pos_mining`)
# MAGIC - `volume_name`       — Volume name under schema (default: `raw_pos_data`)
# MAGIC - `num_transactions`  — Number of transactions to generate (default: `20000`)

# COMMAND ----------
# MAGIC %pip install faker==24.11.0 --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# ── Widget / parameter defaults ───────────────────────────────────────────────
dbutils.widgets.text("catalog",          "pos_demo")
dbutils.widgets.text("schema",           "pos_mining")
dbutils.widgets.text("volume_name",      "raw_pos_data")
dbutils.widgets.text("num_transactions", "20000")

CATALOG         = dbutils.widgets.get("catalog")
SCHEMA          = dbutils.widgets.get("schema")
VOLUME_NAME     = dbutils.widgets.get("volume_name")
NUM_TRANSACTIONS = int(dbutils.widgets.get("num_transactions"))

VOLUME_PATH      = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
RECORDS_PER_FILE = 1_000       # ~1 MB per file
START_DATE_STR   = "2025-05-01"
DAYS             = 92
RANDOM_SEED      = 42

print(f"Target: {VOLUME_PATH}")
print(f"Transactions: {NUM_TRANSACTIONS:,} over {DAYS} days")

# COMMAND ----------
# ── Setup: catalog, schema, volume ────────────────────────────────────────────
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}")

import os
# Clean any existing files in the volume for a fresh run
existing = dbutils.fs.ls(VOLUME_PATH) if os.path.exists(VOLUME_PATH) else []
for f in existing:
    if f.name.endswith(".json") or f.name.endswith(".ndjson"):
        dbutils.fs.rm(f.path)

print("Volume ready.")

# COMMAND ----------
import random
import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from faker import Faker
import math

fake = Faker()
random.seed(RANDOM_SEED)

# ── Constants ─────────────────────────────────────────────────────────────────
START_DATE = datetime(2025, 5, 1, tzinfo=timezone.utc)

CHANNELS = ["IN_STORE_ASSISTED", "SELF_CHECKOUT", "KIOSK"]
SCAN_METHODS = ["BARCODE", "QR_CODE", "MANUAL_ENTRY", "WEIGHT_PLU", "PLU_LOOKUP"]

INCIDENT_TYPES = [
    "ID_CHECK_REQUIRED", "AGE_VERIFICATION", "WEIGHT_SCAN_REQUIRED",
    "QR_CODE_FAILURE", "ITEM_NOT_FOUND", "PRICE_OVERRIDE",
    "MANAGER_APPROVAL_REQUIRED", "COUPON_VALIDATION_FAILED",
    "ITEM_VOID", "TRANSACTION_SUSPENDED",
]

RESOLUTIONS = ["APPROVED", "DECLINED", "OVERRIDE", "SKIPPED", "TIMED_OUT"]
RESOLUTION_WEIGHTS = [0.60, 0.10, 0.15, 0.10, 0.05]

TENDER_TYPES = ["CASH", "CREDIT_CARD", "DEBIT_CARD", "GIFT_CARD", "CHECK",
                "MOBILE_WALLET", "COUPON"]
# Tender mix: CASH 25%, CREDIT 40%, DEBIT 20%, MOBILE 8%, GIFT 5%, COUPON 2%
TENDER_WEIGHTS = [0.25, 0.40, 0.20, 0.05, 0.00, 0.08, 0.02]

CARD_BRANDS = ["VISA", "MASTERCARD", "AMEX", "DISCOVER"]
CARD_BRAND_WEIGHTS = [0.45, 0.30, 0.15, 0.10]

TX_STATUSES = ["COMPLETED", "VOIDED", "SUSPENDED"]
TX_STATUS_WEIGHTS = [0.97, 0.02, 0.01]

CURRENCIES = ["USD"]

# ── Stores ────────────────────────────────────────────────────────────────────
STORES = [
    {"store_id": "STR-001", "name": "Downtown Flagship",
     "channel_weights": [0.70, 0.25, 0.05], "terminals": 8, "cashiers": 12},
    {"store_id": "STR-002", "name": "Midtown Express",
     "channel_weights": [0.50, 0.45, 0.05], "terminals": 5, "cashiers": 6},
    {"store_id": "STR-003", "name": "Eastside Neighborhood",
     "channel_weights": [0.65, 0.30, 0.05], "terminals": 6, "cashiers": 9},
    {"store_id": "STR-004", "name": "Westside Supercenter",
     "channel_weights": [0.60, 0.35, 0.05], "terminals": 10, "cashiers": 15},
    {"store_id": "STR-005", "name": "North Valley Mall",
     "channel_weights": [0.55, 0.40, 0.05], "terminals": 7, "cashiers": 10},
    {"store_id": "STR-006", "name": "South Park Corner",
     "channel_weights": [0.75, 0.20, 0.05], "terminals": 4, "cashiers": 5},
    {"store_id": "STR-007", "name": "Airport Kiosk",
     "channel_weights": [0.10, 0.10, 0.80], "terminals": 3, "cashiers": 2},
]

# ── Product Catalog (220 SKUs) ────────────────────────────────────────────────
def _sku(dept, category, name, price, scan_method="BARCODE",
         age_restricted=False, weighted=False, qr_scannable=False):
    return {
        "sku": f"SKU-{abs(hash(name)) % 90000 + 10000:05d}",
        "item_name": name,
        "department": dept,
        "category": category,
        "default_scan_method": scan_method,
        "unit_price": round(price, 2),
        "age_restricted": age_restricted,
        "weighted": weighted,
        "qr_scannable": qr_scannable,
    }


PRODUCT_CATALOG = [
    # ── Grocery (30) ──────────────────────────────────────────────────────────
    _sku("Grocery","Dairy",        "Organic Whole Milk 1gal",          4.99),
    _sku("Grocery","Dairy",        "2% Reduced Fat Milk 0.5gal",       3.29),
    _sku("Grocery","Dairy",        "Unsalted Butter 1lb",              5.49),
    _sku("Grocery","Dairy",        "Greek Yogurt Plain 32oz",          6.99),
    _sku("Grocery","Dairy",        "Sharp Cheddar Sliced 8oz",         4.49),
    _sku("Grocery","Dairy",        "Cream Cheese 8oz",                 3.49),
    _sku("Grocery","Pantry",       "Whole Grain Bread Loaf",           3.99),
    _sku("Grocery","Pantry",       "Peanut Butter Creamy 16oz",        4.29),
    _sku("Grocery","Pantry",       "Strawberry Jam 18oz",              3.79),
    _sku("Grocery","Pantry",       "Rolled Oats 42oz",                 5.49),
    _sku("Grocery","Pantry",       "Pasta Spaghetti 16oz",             1.89),
    _sku("Grocery","Pantry",       "Marinara Sauce 24oz",              3.29),
    _sku("Grocery","Pantry",       "Chicken Broth 32oz",               2.99),
    _sku("Grocery","Pantry",       "Olive Oil Extra Virgin 16.9fl oz", 9.99),
    _sku("Grocery","Pantry",       "Kosher Salt 26oz",                 2.49),
    _sku("Grocery","Canned Goods", "Diced Tomatoes 14.5oz",            1.39),
    _sku("Grocery","Canned Goods", "Black Beans 15oz",                 1.19),
    _sku("Grocery","Canned Goods", "Tuna in Water 5oz",                2.29),
    _sku("Grocery","Canned Goods", "Coconut Milk 13.5oz",              2.79),
    _sku("Grocery","Frozen",       "Frozen Broccoli Florets 12oz",     2.49),
    _sku("Grocery","Frozen",       "Frozen Pizza Pepperoni 20oz",      7.99),
    _sku("Grocery","Frozen",       "Vanilla Ice Cream 48oz",           6.49),
    _sku("Grocery","Frozen",       "Frozen Edamame 12oz",              3.49),
    _sku("Grocery","Cereal",       "Honey Nut Cheerios 10.8oz",        5.29),
    _sku("Grocery","Cereal",       "Granola with Almonds 16oz",        6.49),
    _sku("Grocery","Snacks",       "Tortilla Chips 10oz",              4.29),
    _sku("Grocery","Snacks",       "Salted Almonds 6oz",               5.99),
    _sku("Grocery","Snacks",       "Pretzels 16oz",                    3.49),
    _sku("Grocery","Snacks",       "Dark Chocolate Bar 3.5oz",         3.99),
    _sku("Grocery","Snacks",       "Fruit Snacks Mixed 22ct",          4.49),

    # ── Produce (25) ──────────────────────────────────────────────────────────
    _sku("Produce","Fruits",       "Banana bunch",          0.59, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Produce","Fruits",       "Apple Fuji per lb",     1.99, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Produce","Fruits",       "Strawberries 1lb",      3.99),
    _sku("Produce","Fruits",       "Blueberries 6oz",       3.49),
    _sku("Produce","Fruits",       "Avocado each",          1.29, scan_method="PLU_LOOKUP"),
    _sku("Produce","Fruits",       "Lemon 3-pack",          2.49),
    _sku("Produce","Fruits",       "Navel Orange per lb",   0.99, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Produce","Fruits",       "Grapes Red Seedless lb",2.99, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Produce","Fruits",       "Mango each",            0.99, scan_method="PLU_LOOKUP"),
    _sku("Produce","Fruits",       "Pineapple each",        2.99, scan_method="PLU_LOOKUP"),
    _sku("Produce","Vegetables",   "Baby Spinach 5oz",      3.49),
    _sku("Produce","Vegetables",   "Romaine Hearts 3-pack", 3.99),
    _sku("Produce","Vegetables",   "Broccoli Crown per lb", 1.69, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Produce","Vegetables",   "Baby Carrots 1lb",      1.99),
    _sku("Produce","Vegetables",   "Cherry Tomatoes 10oz",  3.29),
    _sku("Produce","Vegetables",   "Bell Pepper Red each",  1.29, scan_method="PLU_LOOKUP"),
    _sku("Produce","Vegetables",   "Cucumber each",         0.99, scan_method="PLU_LOOKUP"),
    _sku("Produce","Vegetables",   "Zucchini per lb",       1.49, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Produce","Vegetables",   "Kale bunch",            2.49, scan_method="PLU_LOOKUP"),
    _sku("Produce","Vegetables",   "Sweet Potato per lb",   1.29, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Produce","Vegetables",   "Yellow Onion per lb",   0.99, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Produce","Vegetables",   "Garlic 3-pack",         1.99),
    _sku("Produce","Herbs",        "Fresh Basil bunch",     2.49, scan_method="PLU_LOOKUP"),
    _sku("Produce","Herbs",        "Cilantro bunch",        0.99, scan_method="PLU_LOOKUP"),
    _sku("Produce","Herbs",        "Mint bunch",            1.99, scan_method="PLU_LOOKUP"),

    # ── Deli (20) ─────────────────────────────────────────────────────────────
    _sku("Deli","Sliced Meat",     "Turkey Breast sliced lb",    8.99, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Deli","Sliced Meat",     "Ham Honey Glazed lb",        7.49, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Deli","Sliced Meat",     "Roast Beef lb",              9.99, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Deli","Sliced Meat",     "Salami lb",                  8.49, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Deli","Sliced Cheese",   "Swiss Cheese sliced lb",     7.99, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Deli","Sliced Cheese",   "Provolone Sliced lb",        7.49, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Deli","Prepared",        "Caesar Salad 12oz",          6.99),
    _sku("Deli","Prepared",        "Rotisserie Chicken each",    8.99, scan_method="PLU_LOOKUP"),
    _sku("Deli","Prepared",        "Macaroni Salad lb",          4.99, scan_method="WEIGHT_PLU", weighted=True),
    _sku("Deli","Prepared",        "Chicken Soup 16oz",          5.49),
    _sku("Deli","Sushi",           "Spicy Tuna Roll 8pc",        8.99),
    _sku("Deli","Sushi",           "California Roll 8pc",        7.99),
    _sku("Deli","Sushi",           "Salmon Nigiri 6pc",          9.99),
    _sku("Deli","Hot Foods",       "Chicken Wings 8pc",          7.99, scan_method="PLU_LOOKUP"),
    _sku("Deli","Hot Foods",       "Breakfast Burrito each",     4.99, scan_method="PLU_LOOKUP"),
    _sku("Deli","Hot Foods",       "Soup of the Day 16oz",       4.49, scan_method="PLU_LOOKUP"),
    _sku("Deli","Specialty Cheese","Brie 8oz wheel",             7.99),
    _sku("Deli","Specialty Cheese","Manchego 6oz wedge",         8.49),
    _sku("Deli","Specialty Cheese","Pepper Jack Block 8oz",      4.99),
    _sku("Deli","Pickled",         "Dill Pickles 24oz",          3.49),

    # ── Bakery (20) ───────────────────────────────────────────────────────────
    _sku("Bakery","Bread",         "Sourdough Boule each",       5.49, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Bread",         "Ciabatta Roll 4-pack",       4.49),
    _sku("Bakery","Bread",         "Everything Bagels 6-pack",   4.99),
    _sku("Bakery","Bread",         "Baguette French each",       2.99, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Pastry",        "Croissant Butter each",      2.49, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Pastry",        "Blueberry Muffin each",      2.99, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Pastry",        "Cinnamon Roll each",         3.49, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Pastry",        "Almond Danish each",         2.99, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Cake",          "Chocolate Cake Slice",       4.99, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Cake",          "Cheesecake Slice NY Style",  5.49, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Cake",          "Birthday Cake 8in round",   24.99, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Cookie",        "Chocolate Chip Cookie each", 1.49, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Cookie",        "Snickerdoodle Dozen",        7.99),
    _sku("Bakery","Cookie",        "Macaroon Assorted 6-pack",   8.49),
    _sku("Bakery","Pie",           "Apple Pie 9in",             12.99, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Pie",           "Pumpkin Pie 9in",            9.99, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Donut",         "Glazed Donut each",          1.29, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Donut",         "Donut Box Assorted 12ct",   12.99, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Specialty",     "Focaccia Rosemary slice",    3.99, scan_method="PLU_LOOKUP"),
    _sku("Bakery","Specialty",     "Pretzel Soft each",          2.49, scan_method="PLU_LOOKUP"),

    # ── Beverage (25) ─────────────────────────────────────────────────────────
    _sku("Beverage","Water",       "Spring Water 24-pack",       4.99),
    _sku("Beverage","Water",       "Sparkling Water 12-pack",    6.99),
    _sku("Beverage","Juice",       "Orange Juice 59oz",          4.99),
    _sku("Beverage","Juice",       "Apple Juice 64oz",           3.79),
    _sku("Beverage","Juice",       "Green Smoothie 12oz",        4.49),
    _sku("Beverage","Soda",        "Cola 2L",                    2.29),
    _sku("Beverage","Soda",        "Diet Lemon-Lime 12-pack",    5.99),
    _sku("Beverage","Soda",        "Sparkling Cider 25.4oz",     3.99),
    _sku("Beverage","Coffee",      "Cold Brew Coffee 11oz",      3.99),
    _sku("Beverage","Coffee",      "Espresso Shots 2pk",         2.99),
    _sku("Beverage","Tea",         "Green Tea 16.9oz",           1.99),
    _sku("Beverage","Tea",         "Iced Tea Peach 64oz",        3.49),
    _sku("Beverage","Energy",      "Energy Drink Original 16oz", 3.49),
    _sku("Beverage","Energy",      "Energy Drink Sugar-Free",    3.49),
    _sku("Beverage","Sports",      "Sports Drink 32oz",          2.19),
    _sku("Beverage","Alcohol",     "IPA Craft Beer 6-pack",     10.99, age_restricted=True),
    _sku("Beverage","Alcohol",     "Lager 12-pack",             13.99, age_restricted=True),
    _sku("Beverage","Alcohol",     "Sauvignon Blanc 750ml",     12.99, age_restricted=True),
    _sku("Beverage","Alcohol",     "Cabernet Sauvignon 750ml",  16.99, age_restricted=True),
    _sku("Beverage","Alcohol",     "Prosecco 750ml",            14.99, age_restricted=True),
    _sku("Beverage","Alcohol",     "Vodka 750ml",               22.99, age_restricted=True),
    _sku("Beverage","Alcohol",     "Whiskey Blended 750ml",     29.99, age_restricted=True),
    _sku("Beverage","Alcohol",     "Hard Seltzer Variety 12pk", 16.99, age_restricted=True),
    _sku("Beverage","Tobacco",     "Cigarettes Menthol 20pk",   12.49, age_restricted=True),
    _sku("Beverage","Tobacco",     "Cigars 5-pack",             14.99, age_restricted=True),

    # ── Health & Beauty (25) ──────────────────────────────────────────────────
    _sku("Health & Beauty","Personal Care", "Shampoo Moisturizing 13.5oz",  7.99),
    _sku("Health & Beauty","Personal Care", "Conditioner Deep Repair 13oz", 7.99),
    _sku("Health & Beauty","Personal Care", "Body Wash Lavender 18oz",      5.99),
    _sku("Health & Beauty","Personal Care", "Deodorant Sport 2.6oz",        5.49),
    _sku("Health & Beauty","Personal Care", "Toothpaste Whitening 4oz",     4.99),
    _sku("Health & Beauty","Personal Care", "Floss Mint 40m",               2.99),
    _sku("Health & Beauty","Personal Care", "Razor 5-blade 4-pack",        12.99),
    _sku("Health & Beauty","Personal Care", "Sunscreen SPF50 3oz",          9.99),
    _sku("Health & Beauty","Personal Care", "Lip Balm SPF15 2-pack",        4.49),
    _sku("Health & Beauty","Personal Care", "Hand Lotion Unscented 7.5oz",  5.99),
    _sku("Health & Beauty","Vitamins",      "Vitamin C 1000mg 60ct",        9.99),
    _sku("Health & Beauty","Vitamins",      "Vitamin D3 2000IU 90ct",       8.99),
    _sku("Health & Beauty","Vitamins",      "Omega-3 Fish Oil 60ct",       14.99),
    _sku("Health & Beauty","Vitamins",      "Multivitamin Men 100ct",      12.99),
    _sku("Health & Beauty","OTC",           "Ibuprofen 200mg 100ct",       10.99),
    _sku("Health & Beauty","OTC",           "Antacid Chewable 96ct",        8.49),
    _sku("Health & Beauty","OTC",           "Allergy Relief 24ct",         12.99),
    _sku("Health & Beauty","OTC",           "Cold & Flu Daytime 12ct",     10.49),
    _sku("Health & Beauty","Cosmetics",     "Mascara Lengthening Black",    9.99),
    _sku("Health & Beauty","Cosmetics",     "Foundation Liquid 1oz",       14.99),
    _sku("Health & Beauty","Cosmetics",     "Lip Gloss Clear",              5.99),
    _sku("Health & Beauty","Cosmetics",     "Nail Polish Classic Red",      4.99),
    _sku("Health & Beauty","Cosmetics",     "Eyeshadow Palette 9-color",   16.99),
    _sku("Health & Beauty","Baby",          "Baby Wipes 72ct",              4.99),
    _sku("Health & Beauty","Baby",          "Baby Lotion Fragrance-Free 7oz",5.99),

    # ── Electronics (20) ──────────────────────────────────────────────────────
    _sku("Electronics","Accessories",  "USB-C Cable 6ft",              12.99, qr_scannable=True),
    _sku("Electronics","Accessories",  "USB-A Wall Charger 20W",       14.99, qr_scannable=True),
    _sku("Electronics","Accessories",  "Phone Case Universal 6.1in",   19.99),
    _sku("Electronics","Accessories",  "Screen Protector Tempered 2pk",  9.99),
    _sku("Electronics","Accessories",  "Wireless Earbuds In-Ear",      39.99, qr_scannable=True),
    _sku("Electronics","Accessories",  "Portable Battery 10000mAh",    29.99, qr_scannable=True),
    _sku("Electronics","Accessories",  "MicroSD Card 64GB",            14.99, qr_scannable=True),
    _sku("Electronics","Audio",        "Wired Headphones Over-Ear",    24.99),
    _sku("Electronics","Audio",        "Bluetooth Speaker Mini",       34.99, qr_scannable=True),
    _sku("Electronics","Gaming",       "Gaming Controller USB",        24.99),
    _sku("Electronics","Gaming",       "Gaming Headset Wired",         29.99),
    _sku("Electronics","Gift Cards",   "Digital Gift Card $25",        25.00, qr_scannable=True),
    _sku("Electronics","Gift Cards",   "Digital Gift Card $50",        50.00, qr_scannable=True),
    _sku("Electronics","Gift Cards",   "Digital Gift Card $100",      100.00, qr_scannable=True),
    _sku("Electronics","Batteries",    "AA Batteries 8-pack",           7.99),
    _sku("Electronics","Batteries",    "AAA Batteries 8-pack",          7.99),
    _sku("Electronics","Batteries",    "9V Battery 2-pack",             5.99),
    _sku("Electronics","Smart Home",   "Smart Plug Wi-Fi 2-pack",      24.99, qr_scannable=True),
    _sku("Electronics","Smart Home",   "LED Smart Bulb E26 2-pack",    19.99, qr_scannable=True),
    _sku("Electronics","Media",        "Streaming Stick HDMI",         39.99, qr_scannable=True),

    # ── Apparel (20) ──────────────────────────────────────────────────────────
    _sku("Apparel","Tops",         "Graphic T-Shirt Unisex M",     14.99),
    _sku("Apparel","Tops",         "V-Neck T-Shirt Women S",       12.99),
    _sku("Apparel","Tops",         "Polo Shirt Men M",             24.99),
    _sku("Apparel","Tops",         "Tank Top Athletic Unisex",     11.99),
    _sku("Apparel","Tops",         "Hoodie Zip-Up Unisex L",       34.99),
    _sku("Apparel","Bottoms",      "Jogger Pants Men M",           27.99),
    _sku("Apparel","Bottoms",      "Leggings High-Waist Women M",  22.99),
    _sku("Apparel","Bottoms",      "Shorts Athletic Men M",        19.99),
    _sku("Apparel","Outerwear",    "Rain Jacket Unisex M",         49.99),
    _sku("Apparel","Outerwear",    "Fleece Vest Unisex M",         34.99),
    _sku("Apparel","Accessories",  "Baseball Cap Adjustable",      14.99),
    _sku("Apparel","Accessories",  "Beanie Knit One-Size",          9.99),
    _sku("Apparel","Accessories",  "Scarf Woven Unisex",           16.99),
    _sku("Apparel","Accessories",  "Sunglasses Polarized UV400",   19.99),
    _sku("Apparel","Socks",        "Athletic Socks 6-pair",        12.99),
    _sku("Apparel","Socks",        "Wool Socks 3-pair",            16.99),
    _sku("Apparel","Underwear",    "Cotton Briefs Men 3-pack",     14.99),
    _sku("Apparel","Underwear",    "Sports Bra Women M",           19.99),
    _sku("Apparel","Footwear",     "Flip-Flops Unisex Size 10",    12.99),
    _sku("Apparel","Footwear",     "Sneaker Low-Top Unisex Size 9",49.99),

    # ── Household (20) ────────────────────────────────────────────────────────
    _sku("Household","Cleaning",   "All-Purpose Cleaner 32oz",      4.99),
    _sku("Household","Cleaning",   "Dish Soap Lemon 22oz",          3.49),
    _sku("Household","Cleaning",   "Laundry Detergent Liquid 50oz", 9.99),
    _sku("Household","Cleaning",   "Dryer Sheets 80ct",             4.49),
    _sku("Household","Cleaning",   "Sponges 6-pack",                4.99),
    _sku("Household","Cleaning",   "Paper Towels 6-roll",           8.99),
    _sku("Household","Cleaning",   "Trash Bags 13gal 40ct",         9.99),
    _sku("Household","Paper",      "Toilet Paper 12-roll",          9.99),
    _sku("Household","Paper",      "Facial Tissue 3-box pack",      5.99),
    _sku("Household","Paper",      "Paper Plates 50ct",             4.49),
    _sku("Household","Kitchen",    "Aluminum Foil 75sqft",          4.99),
    _sku("Household","Kitchen",    "Plastic Wrap 200sqft",          4.29),
    _sku("Household","Kitchen",    "Resealable Bags Quart 40ct",    4.99),
    _sku("Household","Kitchen",    "Resealable Bags Gallon 30ct",   4.99),
    _sku("Household","Candles",    "Soy Candle Vanilla 8oz",        9.99),
    _sku("Household","Candles",    "Pillar Candle White 3in",       4.99),
    _sku("Household","Garden",     "Plant Fertilizer All-Purpose",  8.99),
    _sku("Household","Garden",     "Potting Mix 8qt",               7.99),
    _sku("Household","Garden",     "Bug Spray DEET-Free 6oz",       8.49),
    _sku("Household","Storage",    "Storage Bin 18gal Clear",      12.99),

    # ── Floral (15) ───────────────────────────────────────────────────────────
    _sku("Floral","Cut Flowers",   "Rose Bouquet Red 12-stem",     14.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Cut Flowers",   "Mixed Tulips 10-stem",         12.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Cut Flowers",   "Sunflower Bunch 5-stem",        9.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Cut Flowers",   "Lily Asiatic 5-stem",          11.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Cut Flowers",   "Mixed Garden Bouquet",         16.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Cut Flowers",   "Carnation Bunch 12-stem",       7.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Potted Plants", "Succulent Assorted 4in pot",    7.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Potted Plants", "Peace Lily 6in pot",           14.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Potted Plants", "Pothos Golden 4in pot",         9.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Potted Plants", "Orchid Phalaenopsis 5in pot",  19.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Potted Plants", "Herb Garden Kit Basil",         6.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Arrangements",  "Centerpiece Mixed 8in",        34.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Arrangements",  "Mini Bud Vase Arrangement",    12.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Gift",          "Floral Gift Basket",           29.99, scan_method="PLU_LOOKUP"),
    _sku("Floral","Gift",          "Dried Flower Wreath",          24.99, scan_method="PLU_LOOKUP"),
]

print(f"Product catalog: {len(PRODUCT_CATALOG)} SKUs")

# ── Helper functions ──────────────────────────────────────────────────────────

def weighted_choice(population, weights):
    return random.choices(population, weights=weights, k=1)[0]


def rand_ts(base: datetime, offset_min: float, offset_max: float) -> datetime:
    """Return a random datetime between base+offset_min and base+offset_max seconds."""
    offset_secs = random.uniform(offset_min, offset_max)
    return base + timedelta(seconds=offset_secs)


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def gen_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12].upper()}"


def rand_item_count(channel: str, status: str) -> int:
    """Item count distribution: 1-15, fewer for VOIDED/KIOSK."""
    if status == "VOIDED":
        return random.randint(1, 5)
    if channel == "KIOSK":
        return random.randint(1, 6)
    return max(1, int(random.gauss(5.5, 3.2)))


def hourly_weight(hour: int) -> float:
    """Simulate retail store traffic by hour (0-23)."""
    peaks = {
        8: 0.5, 9: 0.8, 10: 1.0, 11: 1.2, 12: 1.5,
        13: 1.4, 14: 1.1, 15: 1.0, 16: 1.1, 17: 1.3,
        18: 1.4, 19: 1.1, 20: 0.8, 21: 0.5, 22: 0.3,
    }
    return peaks.get(hour, 0.1)


def pick_transaction_time() -> datetime:
    """Pick a random datetime distributed over DAYS with hourly traffic weights."""
    day_offset = random.randint(0, DAYS - 1)
    hours = list(range(7, 23))
    weights = [hourly_weight(h) for h in hours]
    hour = weighted_choice(hours, weights)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    ms = random.randint(0, 999)
    return START_DATE + timedelta(
        days=day_offset, hours=hour, minutes=minute, seconds=second,
        milliseconds=ms
    )


# ── Transaction generator ─────────────────────────────────────────────────────

def generate_transaction(tx_idx: int) -> dict:
    store = random.choice(STORES)
    channel = weighted_choice(CHANNELS, store["channel_weights"])
    status = weighted_choice(TX_STATUSES, TX_STATUS_WEIGHTS)
    currency = "USD"
    transaction_id = gen_id("TXN")

    # Assign cashier / terminal
    terminal_num = random.randint(1, store["terminals"])
    terminal_id = f"{store['store_id']}-TERM-{terminal_num:02d}"

    if channel == "SELF_CHECKOUT" or channel == "KIOSK":
        cashier_id = None
    else:
        cashier_num = random.randint(1, store["cashiers"])
        cashier_id = f"{store['store_id']}-CSHR-{cashier_num:03d}"

    # Loyalty customer (70% of assisted, 45% self-checkout, 20% kiosk)
    loyalty_p = {"IN_STORE_ASSISTED": 0.70, "SELF_CHECKOUT": 0.45, "KIOSK": 0.20}
    customer_id = gen_id("CUST") if random.random() < loyalty_p[channel] else None

    # Timing
    start_ts = pick_transaction_time()
    # Duration by channel and status
    if status == "VOIDED":
        duration_secs = random.uniform(30, 180)
    elif status == "SUSPENDED":
        # Suspended: normal scan phase + long gap
        duration_secs = random.uniform(300, 3600)
    elif channel == "KIOSK":
        duration_secs = random.uniform(60, 300)
    elif channel == "SELF_CHECKOUT":
        duration_secs = random.uniform(90, 480)
    else:
        duration_secs = random.uniform(120, 720)
    end_ts = start_ts + timedelta(seconds=duration_secs)

    # Items
    num_items = rand_item_count(channel, status)
    items = _generate_items(transaction_id, start_ts, end_ts, num_items, status)

    # Recalculate totals from items
    subtotal = sum(float(i["extended_amount"]) for i in items if not i["is_void"])
    discount = sum(float(i["line_discount_amount"]) for i in items if not i["is_void"])
    # Simple tax: apply 8.25% on non-grocery items
    taxable = sum(
        float(i["extended_amount"])
        for i in items
        if not i["is_void"] and _is_taxable(i)
    )
    tax = round(taxable * 0.0825, 2)
    total = round(subtotal + tax, 2)
    item_count = len([i for i in items if not i["is_void"] and not i["is_return"]])

    # Incidents
    incidents = _generate_incidents(
        transaction_id, items, start_ts, end_ts, status, channel
    )

    # Tender (only for COMPLETED / SUSPENDED-complete)
    tender = []
    if status == "COMPLETED":
        tender = _generate_tender(transaction_id, total, end_ts)
    elif status == "SUSPENDED":
        # ~70% of SUSPENDED eventually complete and have tender
        if random.random() < 0.7:
            tender = _generate_tender(transaction_id, total, end_ts)

    return {
        "transaction_id": transaction_id,
        "store_id": store["store_id"],
        "terminal_id": terminal_id,
        "cashier_id": cashier_id,
        "customer_id": customer_id,
        "channel": channel,
        "transaction_start_ts": fmt_ts(start_ts),
        "transaction_end_ts": fmt_ts(end_ts),
        "status": status,
        "currency": currency,
        "subtotal_amount": round(subtotal, 2),
        "discount_amount": round(discount, 2),
        "tax_amount": round(tax, 2),
        "total_amount": max(0.0, total),
        "item_count": item_count,
        "items": items,
        "incidents": incidents,
        "tender": tender,
    }


def _is_taxable(item: dict) -> bool:
    taxable_departments = {"Electronics", "Apparel", "Household", "Floral",
                           "Health & Beauty", "Deli", "Bakery"}
    for p in PRODUCT_CATALOG:
        if p["sku"] == item["sku"]:
            return p["department"] in taxable_departments
    return False


def _generate_items(tx_id, start_ts, end_ts, num_items, status):
    items = []
    scan_window = (end_ts - start_ts).total_seconds() * 0.75
    scan_interval = scan_window / max(num_items, 1)

    selected_products = random.choices(PRODUCT_CATALOG, k=num_items)

    for idx, product in enumerate(selected_products):
        line_id = f"LN-{idx + 1:03d}"
        scan_ts = start_ts + timedelta(seconds=scan_interval * idx + random.uniform(0, scan_interval * 0.8))

        # Override scan method for special item types
        if product["weighted"]:
            scan_method = "WEIGHT_PLU"
            quantity = round(random.uniform(0.2, 3.5), 3)
        elif product["qr_scannable"] and random.random() < 0.30:
            scan_method = "QR_CODE"
            quantity = round(random.uniform(1, 3), 3)
        else:
            scan_method = product["default_scan_method"]
            quantity = 1.0 if scan_method not in {"WEIGHT_PLU"} else round(random.uniform(0.2, 3.5), 3)

        unit_price = product["unit_price"]
        line_discount = round(unit_price * random.choice([0.0, 0.0, 0.0, 0.0, 0.05, 0.10, 0.15]), 2)
        extended = round(float(quantity) * unit_price - line_discount, 2)

        is_void = status == "VOIDED" and idx == num_items - 1  # last item in voided tx
        is_return = random.random() < 0.02  # 2% return rate

        items.append({
            "line_id": line_id,
            "sequence_number": idx + 1,
            "scan_ts": fmt_ts(scan_ts),
            "sku": product["sku"],
            "item_name": product["item_name"],
            "department": product["department"],
            "category": product["category"],
            "scan_method": scan_method,
            "quantity": float(round(quantity, 3)),
            "unit_price": unit_price,
            "line_discount_amount": line_discount,
            "extended_amount": extended,
            "is_return": is_return,
            "is_void": is_void,
        })

    return items


def _generate_incidents(tx_id, items, start_ts, end_ts, status, channel):
    incidents = []
    session_cashier = f"MGR-{random.randint(100, 999)}"  # manager for approvals

    # Rule 1: Age-restricted items → ID_CHECK_REQUIRED + AGE_VERIFICATION
    for item in items:
        for product in PRODUCT_CATALOG:
            if product["sku"] == item["sku"] and product["age_restricted"]:
                base_ts = _parse_ts(item["scan_ts"]) + timedelta(seconds=random.uniform(1, 5))
                for inc_type in ["ID_CHECK_REQUIRED", "AGE_VERIFICATION"]:
                    resolution_secs = random.randint(10, 60)
                    resolution = weighted_choice(["APPROVED", "DECLINED"], [0.90, 0.10])
                    resolution_ts = base_ts + timedelta(seconds=resolution_secs)
                    if resolution_ts > end_ts:
                        resolution_ts = end_ts - timedelta(seconds=5)
                    incidents.append(_make_incident(
                        tx_id, inc_type, base_ts, resolution_ts,
                        item["line_id"], resolution, session_cashier
                    ))
                    base_ts += timedelta(seconds=resolution_secs + 2)
                break

    # Rule 2: Weighted/produce items → WEIGHT_SCAN_REQUIRED (~80%)
    for item in items:
        if item["scan_method"] == "WEIGHT_PLU" and random.random() < 0.80:
            base_ts = _parse_ts(item["scan_ts"]) + timedelta(seconds=random.uniform(1, 3))
            resolution_secs = random.randint(5, 30)
            resolution_ts = base_ts + timedelta(seconds=resolution_secs)
            if resolution_ts < end_ts:
                incidents.append(_make_incident(
                    tx_id, "WEIGHT_SCAN_REQUIRED", base_ts, resolution_ts,
                    item["line_id"], "APPROVED", None
                ))

    # Rule 3: QR_CODE items → QR_CODE_FAILURE (~5%)
    for item in items:
        if item["scan_method"] == "QR_CODE" and random.random() < 0.05:
            base_ts = _parse_ts(item["scan_ts"]) + timedelta(seconds=random.uniform(1, 2))
            resolution_secs = random.randint(15, 90)
            resolution_ts = base_ts + timedelta(seconds=resolution_secs)
            if resolution_ts < end_ts:
                incidents.append(_make_incident(
                    tx_id, "QR_CODE_FAILURE", base_ts, resolution_ts,
                    item["line_id"],
                    weighted_choice(["OVERRIDE", "SKIPPED"], [0.7, 0.3]),
                    session_cashier
                ))

    # Rule 4: MANAGER_APPROVAL_REQUIRED (~3% of transactions)
    if random.random() < 0.03:
        base_ts = rand_ts(start_ts, 30, max(60, duration_total(start_ts, end_ts) * 0.5))
        resolution_secs = random.randint(30, 180)
        resolution_ts = base_ts + timedelta(seconds=resolution_secs)
        if resolution_ts < end_ts:
            incidents.append(_make_incident(
                tx_id, "MANAGER_APPROVAL_REQUIRED", base_ts, resolution_ts,
                None, weighted_choice(["APPROVED", "DECLINED"], [0.85, 0.15]),
                session_cashier
            ))

    # Rule 5: ITEM_NOT_FOUND (~5% of transactions)
    if random.random() < 0.05 and items:
        item = random.choice(items)
        base_ts = _parse_ts(item["scan_ts"]) + timedelta(seconds=random.uniform(2, 8))
        resolution_secs = random.randint(20, 120)
        resolution_ts = base_ts + timedelta(seconds=resolution_secs)
        if resolution_ts < end_ts:
            incidents.append(_make_incident(
                tx_id, "ITEM_NOT_FOUND", base_ts, resolution_ts,
                item["line_id"],
                weighted_choice(["OVERRIDE", "SKIPPED"], [0.6, 0.4]),
                session_cashier
            ))

    # Rule 6: PRICE_OVERRIDE (~3% of transactions)
    if random.random() < 0.03 and items:
        item = random.choice(items)
        base_ts = _parse_ts(item["scan_ts"]) + timedelta(seconds=random.uniform(2, 5))
        resolution_secs = random.randint(15, 60)
        resolution_ts = base_ts + timedelta(seconds=resolution_secs)
        if resolution_ts < end_ts:
            incidents.append(_make_incident(
                tx_id, "PRICE_OVERRIDE", base_ts, resolution_ts,
                item["line_id"], "APPROVED", session_cashier
            ))

    # Rule 7: COUPON_VALIDATION_FAILED (~2% of transactions)
    if random.random() < 0.02:
        base_ts = rand_ts(start_ts, duration_total(start_ts, end_ts) * 0.7,
                          duration_total(start_ts, end_ts) * 0.9)
        resolution_secs = random.randint(10, 45)
        resolution_ts = base_ts + timedelta(seconds=resolution_secs)
        if resolution_ts < end_ts:
            incidents.append(_make_incident(
                tx_id, "COUPON_VALIDATION_FAILED", base_ts, resolution_ts,
                None,
                weighted_choice(["DECLINED", "OVERRIDE", "SKIPPED"], [0.5, 0.3, 0.2]),
                session_cashier
            ))

    # Rule 8: ITEM_VOID for voided items
    for item in items:
        if item["is_void"]:
            base_ts = _parse_ts(item["scan_ts"]) + timedelta(seconds=random.uniform(2, 10))
            resolution_secs = random.randint(5, 20)
            resolution_ts = base_ts + timedelta(seconds=resolution_secs)
            incidents.append(_make_incident(
                tx_id, "ITEM_VOID", base_ts, resolution_ts,
                item["line_id"], "APPROVED", session_cashier
            ))

    # Anomaly: ~10% of incident-bearing transactions repeat an incident type
    if incidents and random.random() < 0.10:
        original = random.choice(incidents)
        repeat_ts = _parse_ts(original["resolution_ts"]) + timedelta(seconds=random.randint(30, 120))
        if repeat_ts < end_ts:
            resolution_secs = random.randint(10, 60)
            repeat_resolution_ts = repeat_ts + timedelta(seconds=resolution_secs)
            if repeat_resolution_ts < end_ts:
                incidents.append(_make_incident(
                    tx_id, original["incident_type"], repeat_ts, repeat_resolution_ts,
                    original.get("related_line_id"),
                    weighted_choice(RESOLUTIONS, RESOLUTION_WEIGHTS),
                    session_cashier
                ))

    return incidents


def _make_incident(tx_id, inc_type, incident_ts, resolution_ts,
                   related_line_id, resolution, resolved_by):
    dur = int((resolution_ts - incident_ts).total_seconds())
    return {
        "incident_id": gen_id("INC"),
        "incident_ts": fmt_ts(incident_ts),
        "incident_type": inc_type,
        "related_line_id": related_line_id,
        "resolution": resolution,
        "resolution_ts": fmt_ts(resolution_ts),
        "resolved_by": resolved_by,
        "duration_seconds": max(0, dur),
    }


def _generate_tender(tx_id, total_amount, end_ts):
    tender = []
    remaining = total_amount

    # Primary tender type
    primary_type = weighted_choice(TENDER_TYPES, TENDER_WEIGHTS)

    # Split tender: ~8% chance of second payment method
    if random.random() < 0.08 and total_amount > 10:
        split = round(random.uniform(5, min(total_amount * 0.5, 50)), 2)
        remaining = round(total_amount - split, 2)

        secondary_type = weighted_choice(
            ["CASH", "GIFT_CARD", "COUPON"], [0.5, 0.3, 0.2]
        )
        tender.append(_make_tender(tx_id, secondary_type, split,
                                   end_ts - timedelta(seconds=30)))

    tender.append(_make_tender(tx_id, primary_type, remaining, end_ts - timedelta(seconds=15)))

    return tender


def _make_tender(tx_id, tender_type, amount, tender_ts):
    auth_number = None
    card_brand = None
    last4 = None
    coupon_code = None
    change_amount = None

    if tender_type == "CASH":
        overpay = random.choice([0, 0, 0, 0.50, 1.00, 5.00, 10.00])
        cash_given = amount + overpay
        change_amount = round(cash_given - amount, 2) if overpay > 0 else None

    elif tender_type in ("CREDIT_CARD", "DEBIT_CARD"):
        auth_number = f"AUTH-{random.randint(100000, 999999)}"
        card_brand = weighted_choice(CARD_BRANDS, CARD_BRAND_WEIGHTS)
        last4 = f"{random.randint(1000, 9999)}"

    elif tender_type == "GIFT_CARD":
        auth_number = f"GC-{uuid.uuid4().hex[:8].upper()}"
        last4 = f"{random.randint(1000, 9999)}"

    elif tender_type == "COUPON":
        coupon_code = f"COUP-{uuid.uuid4().hex[:6].upper()}"

    elif tender_type == "MOBILE_WALLET":
        auth_number = f"MW-{uuid.uuid4().hex[:10].upper()}"

    return {
        "tender_id": gen_id("TND"),
        "tender_ts": fmt_ts(tender_ts),
        "tender_type": tender_type,
        "amount": round(amount, 2),
        "authorization_number": auth_number,
        "card_brand": card_brand,
        "last4": last4,
        "coupon_code": coupon_code,
        "change_amount": change_amount,
    }


def _parse_ts(ts_str: str) -> datetime:
    return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def duration_total(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds()


# COMMAND ----------
# ── Generate transactions and write to volume ─────────────────────────────────
import math

print(f"Generating {NUM_TRANSACTIONS:,} transactions...")

transactions = []
for i in range(NUM_TRANSACTIONS):
    transactions.append(generate_transaction(i))
    if (i + 1) % 1000 == 0:
        print(f"  Generated {i + 1:,} / {NUM_TRANSACTIONS:,}")

print(f"Done generating. Writing to {VOLUME_PATH}...")

# Write in batches as NDJSON files
num_files = math.ceil(NUM_TRANSACTIONS / RECORDS_PER_FILE)
for file_idx in range(num_files):
    start = file_idx * RECORDS_PER_FILE
    end = min(start + RECORDS_PER_FILE, NUM_TRANSACTIONS)
    batch = transactions[start:end]

    file_path = f"{VOLUME_PATH}/pos_transactions_{file_idx:04d}.json"
    content = "\n".join(json.dumps(rec, default=str) for rec in batch)

    with open(file_path, "w") as f:
        f.write(content)

    print(f"  Wrote file {file_idx + 1}/{num_files}: {file_path} ({len(batch)} records)")

print(f"\nAll {NUM_TRANSACTIONS:,} transactions written to {VOLUME_PATH}")
print(f"Files: {num_files}")

# COMMAND ----------
# ── Validation summary ─────────────────────────────────────────────────────────
status_counts = {}
incident_counts = {}
tender_counts = {}
channel_counts = {}

for tx in transactions:
    s = tx["status"]
    status_counts[s] = status_counts.get(s, 0) + 1
    for inc in tx["incidents"]:
        t = inc["incident_type"]
        incident_counts[t] = incident_counts.get(t, 0) + 1
    for tnd in tx["tender"]:
        t = tnd["tender_type"]
        tender_counts[t] = tender_counts.get(t, 0) + 1
    c = tx["channel"]
    channel_counts[c] = channel_counts.get(c, 0) + 1

print("=== Generation Summary ===")
print(f"\nTotal transactions: {NUM_TRANSACTIONS:,}")
print("\nStatus distribution:")
for k, v in sorted(status_counts.items()):
    print(f"  {k}: {v:,} ({v/NUM_TRANSACTIONS:.1%})")
print("\nChannel distribution:")
for k, v in sorted(channel_counts.items()):
    print(f"  {k}: {v:,} ({v/NUM_TRANSACTIONS:.1%})")
print("\nIncident type distribution:")
for k, v in sorted(incident_counts.items(), key=lambda x: -x[1]):
    print(f"  {k}: {v:,}")
print("\nTender type distribution:")
for k, v in sorted(tender_counts.items(), key=lambda x: -x[1]):
    print(f"  {k}: {v:,}")

transactions_with_incidents = sum(1 for tx in transactions if tx["incidents"])
print(f"\nTransactions with incidents: {transactions_with_incidents:,} ({transactions_with_incidents/NUM_TRANSACTIONS:.1%})")
