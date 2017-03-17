DROP TABLE IF EXISTS SY_ITEM_DPITEM;
CREATE TABLE SY_ITEM_DPITEM
(
	COLL_ID BIGINT
	, ITEM_ID BIGINT
	, DPITEM_ID BIGINT
);

JAR SY_ITEM_DP.JAR;

DROP TABLE IF EXISTS SY_ICT_DPICT;
CREATE TABLE SY_ICT_DPICT AS
SELECT
	COLL_ID
	, A.ITEM_ID
	, A.CAT_ID
	, A.TERMS
	, DPITEM_ID
	, SY_ITEMS.CAT_ID AS DPITEM_CAT_ID
	, SY_ITEMS.TERMS AS DPITEM_TERMS
FROM
(
	SELECT 
		COLL_ID
		, SY_ITEM_DPITEM.ITEM_ID
		, CAT_ID
		, TERMS
		, DPITEM_ID
	FROM SY_ITEM_DPITEM
	LEFT OUTER JOIN SY_ITEMS
	ON SY_ITEM_DPITEM.ITEM_ID = SY_ITEMS.ITEM_ID
)A
LEFT OUTER JOIN SY_ITEMS
ON A.DPITEM_ID = SY_ITEMS.ITEM_ID;

DROP TABLE IF EXISTS SY_ITEM_DPITEMS;
CREATE TABLE SY_ITEM_DPITEMS AS
SELECT
	ITEM_ID
	, CAT_ID
	, PASTE_UNIQUE(DPITEM_ID) AS DPITEMS
FROM SY_ICT_DPICT;
GROUP BY ITEM_ID, CAT_ID;

DROP TABLE IF EXISTS SY_CAT_DPCATS;
CREATE TABLE SY_CAT_DPCATS AS
SELECT
	CAT_ID
	, PASTE_TABLE(CAST(DPITEM_CAT_ID AS STRING)) AS DPCATS
FROM SY_ICT_DPICT
GROUP BY CAT_ID;

DROP TABLE IF EXISTS SY_CAT_DPCAT;
CREATE TABLE SY_CAT_DPCAT AS
SELECT
	CAT_ID
	, DPITEM_CAT_ID
FROM SY_ICT_DPICT
GROUP BY CAT_ID, DPITEM_CAT_ID;

DROP TABLE IF EXISTS SY_ITEM_TERM;
CREATE TABLE SY_ITEM_TERM
(
	ITEM_ID BIGINT
	, CAT_ID BIGINT
	, TERM_ID BIGINT
);

JAR SY_ITEM_TERM.JAR;

DROP TABLE IF EXISTS SY_TERM_CAT_MS;
CREATE TABLE SY_TERM_CAT_MS AS
SELECT
	TERM_ID
	, A.CAT_ID
	, TERM_CAT_S
	, TERM_CAT_S / CAT_S AS TERM_CAT_MS
FROM
(
	SELECT
		TERM_ID
		, CAT_ID
		, COUNT(*) AS TERM_CAT_S
	FROM SY_ITEM_TERM
	GROUP BY TERM_ID, CAT_ID
)A

LEFT OUTER JOIN
(
	SELECT CAT_ID, COUNT(*) AS CAT_S
	FROM SY_ITEM_TERM
	GROUP BY CAT_ID
)B
ON A.CAT_ID = B.CAT_ID;

DROP TABLE IF EXISTS SY_ITEM_TERMS;
CREATE TABLE SY_ITEM_TERMS AS
SELECT
	ITEM_ID
	, SY_ITEM_TERM.CAT_ID
	, WM_CONCAT(",", CONCAT(SY_ITEM_TERM.TERM_ID, ":", TERM_CAT_MS)) AS TERMMS
FROM SY_ITEM_TERM
LEFT OUTER JOIN SY_TERM_CAT_MS
ON SY_ITEM_TERM.TERM_ID = SY_TERM_CAT_MS.TERM_ID AND SY_ITEM_TERM.CAT_ID = SY_TERM_CAT_MS.CAT_ID
GROUP BY ITEM_ID, SY_ITEM_TERM.CAT_ID;
