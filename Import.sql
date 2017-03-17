DROP TABLE IF EXISTS DIM_FASHION_MATCH_SETS;
CREATE TABLE DIM_FASHION_MATCH_SETS AS
SELECT * FROM TIANCHI_FM.DIM_FASHION_MATCH_SETS;

DROP TABLE IF EXISTS DIM_ITEMS;
CREATE TABLE DIM_ITEMS AS
SELECT * FROM TIANCHI_FM.DIM_ITEMS;

DROP TABLE IF EXISTS USER_BOUGHT_HISTORY;
CREATE TABLE USER_BOUGHT_HISTORY AS
SELECT * FROM TIANCHI_FM.USER_BOUGHT_HISTORY;

DROP TABLE IF EXISTS TEST_ITEMS;
CREATE TABLE TEST_ITEMS AS
SELECT * FROM TIANCHI_FM.TEST_ITEMS;