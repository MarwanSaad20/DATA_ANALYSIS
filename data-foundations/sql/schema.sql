CREATE TABLE IF NOT EXISTS sales (
    order_id INTEGER PRIMARY KEY,
    product TEXT NOT NULL,
    category TEXT NOT NULL,
    quantity INTEGER CHECK(quantity > 0),
    unit_price REAL CHECK(unit_price > 0),
    region TEXT NOT NULL
);
