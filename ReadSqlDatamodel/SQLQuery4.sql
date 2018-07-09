SELECT
    *,
    OBJECT_NAME(object_id) As Table_Name,
	OBJECT_SCHEMA_NAME(object_id) as Schema_Name
FROM
    sys.indexes
WHERE
	is_hypothetical = 0 AND
    index_id != 0 AND
    object_id = OBJECT_ID('RDD.ATT_VERWYZING');  
GO