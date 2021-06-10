CREATE TABLE records (
  id INTEGER PRIMARY KEY,
  timestamp INT NOT NULL,
  delivered_1 REAL,
  delivered_2 REAL,
  received_1 REAL,
  received_2 REAL,
  current_tariff INT,
  actual_delivered REAL,
  actual_received REAL,
  max_power REAL,
  switch_mode INT
);
