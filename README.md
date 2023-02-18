# North Star Metric: Customer Retention Rate 

This is the project!

Extract from the table 
| changeDate  | payerId | recordType |entityId | valCurr | valPrev |  
| ------------------------------------|---| -----------------| --------------- |------------| ------------- |
| timestamp '2012-10-31 01:00:00.000' | str 'payment_method' | str 'rid04XXXXXXXXXX'  | str 'eid04XXXXXXXXXX' | 12 | 6
| timestamp '2012-10-31 01:00:00.000' | str 'subscription_type' | str 'rid01XXXXXXXXXX'  | str 'eid01XXXXXXXXXX' | 12 |11


```sql
WITH 
    change_trail  
AS ( -- all changes to the payment method involving payment method of interest -> 12 
    SELECT
        *,
        CASE WHEN is_join = True THEN row_number + 1 ELSE NULL END record_end_row_number   -- expected row number for the churn 
    FROM 
        ( -- get all the changes that involve payment method 12 
        SELECT DISTINCT 
            c.entityId,
            CASE WHEN c.valCurr  = '12' AND c.valPrev != '12' THEN c.changeDate     END join_date,
            CASE WHEN c.valCurr  = '12' AND c.valPrev != '12' THEN True ELSE False  END is_join,  -- flag to signify join
            CASE WHEN c.valCurr != '12' AND c.valPrev  = '12' THEN c.changeDate     END churn_date,
            rank() OVER (PARTITION BY c.entityId ORDER BY c.changeDce)                  row_number -- since there are duplicates in the table 
        FROM 
            AWSDataCatalog.data_warehouse.changeTrail c
        JOIN
            AWSDataCatalog.data_warehouse.payer p
            ON p.id = c.payorId  
            AND p.channel_source IN (16, 26, 30) -- sources of the subscription
        WHERE 
            True
            AND c.recordType = 'payment_method'
            AND c.partitionId >= date '2017-06-15' 
            AND regexp_like(entityId, 'eid0[1-4].{10}')
        )
    ), 
    join_churn_pairs
AS ( -- give all adjacent pairs of join and subsequent churn 
    SELECT 
        *,
        CASE WHEN last_join_date = join_date AND churn_date IS NULL THEN current_date ELSE churn_date END filled_churn_date -- current date if did not churn
    FROM 
            ( -- get joinment and respective churn date (if any)
            SELECT 
                j.entityId,
                ch.join_date,
                ch.churn_date,
                max(j.join_date) OVER (PARTITION BY j.entityId) last_join_date                                   
            FROM 
                change_trail j -- join
            LEFT JOIN -- not all have churned 
                change_trail ch -- churns 
                ON ch.entityId = j.entityId 
                AND ch.record_end_row_number = j.row_number 
            )
        WHERE 
            True
            AND joindate IS NOT NULL -- remove redundant pairs from the left join  
    ),
    last_day_of_month
AS ( -- calcualate ordinal number of the last day of a given month
    SELECT
        date_trunc('Month', full_date) Month,
        day_of_month(max(full_date)) day_number
    FROM 
        AWSDataCatalog.fullDateInfo -- table built with Python in Glue job to give info about a given day 
    WHERE 
        True 
        AND full_date >= date '2017-06-15'  
        AND full_date < current_date + interval '1' month -- filter the rest of the table 
    GROUP BY 
        1
    ),
    joined_vendors
AS ( -- get all end of month payees that were transactable
    SELECT
        date_trunc('Month', dd.full_date)   Month, 
        count(DISTINCT entityId)            EOM
    FROM
        join_churn_pairs
    JOIN 
        AWSDataCatalog.fullDateInfo f 
        ON f.full_date >= date '2021-12-01' -- 1-month lookup from current year
        AND f.full_date < current_date 
        AND f.full_date >= date(join_date) 
        AND f.full_date < date(filled_churn_date)
    JOIN
        last_day_of_month ldm
        ON ldm.Month = date_trunc('Month', d.full_date) 
        AND ldm.day_number = d.month_day_number
    WHERE
        True
    GROUP BY 
        1
    ),
    changes 
AS ( -- count of distinct payees that changed the payment method in a month
    SELECT
        date_trunc('Month', c.changeDate) Month,
        count(DISTINCT CASE WHEN c.valCurr = '12' AND c.valPrev <> '12' THEN c.entityId END) joined,
        count(DISTINCT CASE WHEN c.valCurr <> '12' AND c.valPrev = '12' THEN c.entityId END) churnd
    FROM 
        AWSDataCatalog.data_warehouse.changeTrail at
    JOIN
        AWSDataCatalog.data_warehouse.payer p
        ON p.id = c.payorId  
        AND p.channel_source IN (16, 26, 30) -- sources of the subscription
    WHERE 
        True
        AND c.recordType = 'payment_method'
        AND c.partitionId >= '2017-06-15' 
    GROUP BY 
        1
    ),
    summary
AS ( -- add all metrics 
    SELECT
        *,
        lag(EOM, 1) OVER (ORDER BY Month) BOM -- beginning of the months are the transactable payees that existed on the last day of the month
    FROM 
        joined_vendors
    LEFT JOIN 
        changes 
        USING (Month)
    )

SELECT 
    Month,
    BOM,
    churnd,
    joined,
    EOM
FROM 
    summary
```

After transposing the result:
|Month|January 23|February 23| March 23| April 23| May 23|
Beginning of the Month|	1,000,584 |	1,048,854 |	1,116,901| 1,159,974| 1,235,288|
Churned	|				2,971	| 19,164 |	45,586 |	17,998	| 12,476|
Joined 	|				61,310 |	87,076	|87,974 |	93,265	|53,605|
End of the Month |		1,048,854	|1,116,901|	1,159,974	|1,235,288|	1,276,436|


USING dbt 
```sql
    join_churn_pairs
AS ( -- give all adjacent pairs of join and subsequent churn 
    SELECT 
        *,
        CASE WHEN last_join_date = join_date AND churn_date IS NULL THEN current_date ELSE churn_date END filled_churn_date -- current date if did not churn
    FROM 
            ( -- get joinment and respective churn date (if any)
            SELECT 
                j.entityId,
                ch.join_date,
                ch.churn_date,
                max(j.join_date) OVER (PARTITION BY j.entityId) last_join_date                                   
            FROM 
                change_trail j -- join
            LEFT JOIN -- not all have churned 
                change_trail ch -- churns 
                ON ch.entityId = j.entityId 
                AND ch.record_end_row_number = j.row_number 
            )
        WHERE 
            True
            AND joindate IS NOT NULL -- remove redundant pairs from the left join  
    ),
```
