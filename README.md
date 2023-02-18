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
        (
        SELECT DISTINCT  
            *,
            rank() OVER (PARTITION BY c.entityId ORDER BY c.changeDce) row_number -- rank since there are duplicates in the table
        FROM 
            (
                ( -- get all the changes that involve payment method 12 
                SELECT  
                    c.entityId,
                    CASE WHEN c.valCurr  = '12' AND c.valPrev != '12' THEN c.changeDate     END join_date,
                    CASE WHEN c.valCurr  = '12' AND c.valPrev != '12' THEN True ELSE False  END is_join,  -- flag to signify join
                    CASE WHEN c.valCurr != '12' AND c.valPrev  = '12' THEN c.changeDate     END churn_date
                FROM 
                    AWSDataCatalog.data_warehouse.changeTrail c
                JOIN
                    AWSDataCatalog.data_warehouse.payer p
                    ON p.id = c.payerId  
                    AND p.channel_source IN (16, 26, 30) -- sources of the subscription
                WHERE 
                    True
                    AND c.recordType = 'payment_method'
                    AND c.partitionId >= date '2017-06-15' 
                    AND regexp_like(entityId, 'eid0[1-4].{10}')
                )
                UNION
                ( -- get the manual updates that are not recorded in change log
                SELECT 
                    merchant_id         entityId,
                    date '2023-01-06'   join_date,
                    True                is_join,
                    NULL                churn_date
                FROM 
                    AWSDataCatalog.data_warehouse.dmytro_scratch_merchants_incident -- the manual ETL was done here 
                WHERE 
                    True 
                )
                UNION
                ( -- get the manual updates from the payor side that are not recorded in change log
                SELECT 
                    m.id                entityId,
                    NULL                join_date
                    False               is_join,
                    date '2019-06-06'   churn_date
                 FROM
                    AWSDataCatalog.data_warehouse.mercaht m
                 JOIN
                    AWSDataCatalog.data_warehouse.payer p 
                    ON p.id = m.payerId 
                    AND p.channel_source IN (16, 26, 30) -- sources of the subscription
                    AND p.parent_payer_id IN ('rid04gfs54g5df48', 'rid02gfqw45ytudds') 
                )
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
        AWSDataCatalog.glue.fullDateInfo -- table built with Python in Glue job to give info about a given day 
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
        date_trunc('Month', f.full_date)   Month, 
        count(DISTINCT entityId)            EOM
    FROM
        join_churn_pairs
    JOIN 
        AWSDataCatalog.glue.fullDateInfo f 
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

<table style="text-align:center; width:30%; text-align:center;font-size: 100% ">
  <caption style = "font-size: 100%">SQL Output</caption>
  <tr>
    <th>Month</th>
    <th>January 23</th>
    <th>February 23</th>
    <th>March 23</th>
    <th>April 23</th>
    <th>May 23</th>
    <th>June 23</th>
  </tr>
  <tr>
    <td>BOM</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
  </tr>
  <tr>
    <td>Joined</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
  </tr>
  <tr>
    <td>Churned</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
  </tr>
   <tr>
    <td>EOM</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
  </tr>
</table>

USING dbt 

![alt text](https://github.com/dymytryo/githubTest/blob/d77cc97247fc3c8392b99d3ebbd025081ff05d25/dbt_modeling_flow.png?raw=true)

Now we have the retention detail stored and we can query further with no issues 

Result of dbt-izing: 
<table style="text-align:center; width:30%; text-align:center;font-size: 100% ">
  <caption style = "font-size: 100%">SQL Output</caption>
  <tr>
    <th>Month</th>
    <th>January 23</th>
    <th>February 23</th>
    <th>March 23</th>
    <th>April 23</th>
    <th>May 23</th>
    <th>June 23</th>
  </tr>
  <tr>
    <td>merchant_id</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
  </tr>
  <tr>
    <td>transaction_volume</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
  </tr>
  <tr>
    <td>dollar_volume</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
  </tr>
   <tr>
    <td>payment_method</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
  </tr>
</table>


<table style="text-align:center; width:30%; text-align:center;font-size: 100% ">
  <caption style = "font-size: 100%">SQL Output</caption>
  <tr>
    <th>merchant_id</th>
    <th>join_date</th>
    <th>churn_date</th>
  </tr>
  <tr>
    <td>dfsddsf</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
  </tr>
  <tr>
    <td>dsgdgdse</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
    <td>1,000,584</td>
    <td>1,048,854</td>
    <td>1,116,901</td>
  </tr>
  </tr>
</table>


![alt text](https://github.com/dymytryo/githubTest/blob/b95e10d6b101626e6a550a7825bacfe5b4f00848/churned_joined_merchants.png?raw=true)

```sql
WITH 
    transactING_merchant_agg
AS ( -- get all merchants that received a payment in month M
    SELECT
                                     M,
        map_agg(merchant_id, transaction_volume)    transaction_volume_M,
        array_agg(DISTINCT payor_id) payors_M
    FROM 
        AWSDataCatalog.dbt_marts.payment_detail_agg 
    GROUP BY 
        1
    ),
    transactABLE_merchant_agg
AS ( -- get all merchants that could receive a payment in a month M
    SELECT
        f.full_date M,
        map_agg(merchant_id, 0) possible_transaction_volume_M
    FROM
        AWSDataCatalog.dbt_marts.merchant_retention_record 
    JOIN 
        AWSDataCatalog.glue.fullDateInfo f 
        ON f.full_date >= date '2021-12-01' -- 1-month lookup from current year
        AND f.full_date < current_date 
        AND f.full_date >= date(join_date) 
        AND f.full_date < date(churn_date)
        AND f.month_day_number = 1 
    WHERE
        True
    GROUP BY 
        1
    )
SELECT 
    M Month,
    -- get percentage of merchants that received a payment in M - 3 (three months ago) in M (month of interest)
    1e0*cardinality(array_intersect(map_keys(lag(transaction_volume_M, 3) OVER (ORDER BY M)),
                                    map_keys(transaction_volume_M))
    ) / cardinality(map_keys(lag(merchant_transaction_volume_M, 3) OVER (ORDER BY M))) "Retained Merchants in M from M-3",
    -- get percentage of payors that sent a payment in M - 3 in M
    1e0*cardinality(array_intersect(lag(payors_M, 3) OVER (ORDER BY M),
                                    payors_M)
    )/cardinality(lag(payors_M, 3) OVER (ORDER BY M)) "Retained Payors in M from M-3",
    -- get percentage of retained volume from transactions in M - 3 in M 
    cardinality(map_values(map_filter(transaction_volume_M, (k, v) -> contains(
    array_intersect(map_keys(transaction_volume_M),
                    map_keys(lag(transaction_volume_M, 3) OVER (ORDER BY M)), k)))) "Retained Volume for TransactING Merchants in M from M-3",
    -- sanity check: get percentage of retained volume from transactions in M - 3 in M 
    1e0*reduce(map_values(map_zip_with(transaction_volume_M,
                                       lag(transaction_volume_M, 3) OVER (ORDER BY M),
                                       (k, v1, v2) -> IF(v1 IS NULL OR v2 IS NULL, NULL, v1))), 0, (s, x) -> IF(x IS NULL, s, s + x), s -> s
    ) / reduce(map_values(lag(transaction_volume_M, 3) OVER (ORDER BY M)), 0, (s, x) -> IF(x IS NULL, s, s + x), s -> s) 
    "Retained Volume for TransactING Merchants in M from M-3",
   -- get percentage of retained volume from transactable vendors in M - 3 in M 
   1e0*reduce(map_values(map_zip_with(transaction_volume_M,
                                      possible_transaction_volume_M,
                                      (k, v1, v2) -> IF(v2 IS NOT NULL, NULL, v1))), 0, (s, x) -> IF(x IS NULL, s, s + x), s -> s
   )/ reduce(map_values(lag(transaction_volume_M, 3) OVER (ORDER BY M)), 0, (s, x) -> IF(x IS NULL, s, s + x), s -> s)
   "Retained Volume from Transactable Merchants in M",
   -- get distinct count of the merchants that transacted in m 
   cardinality(transaction_volume_M) "Transacting Merchants in M",
   -- get distinct count of the merchants that could received a transaction in M
   cardinality(possible_transaction_volume_M) "Transactable Merchants in M",
   -- get dollar amount of the volume that was generated by transactING merchants in M 
   reduce(map_values(transaction_volume_M), 0, (s, x) -> IF(x IS NULL, s, s + x), s -> s) "Volume from Transacting in M",
   -- get dollar amount of the volume that was generated by transactABLE merchants in M
   reduce(map_values(map_zip_with(transaction_volume_M, possible_transaction_volume_M, (k, v1, v2) -> IF(v2 IS NOT NULL, NULL, v1))), 0, (s, x) -> IF(x IS NULL, s, s + x), s -> s) "Volume from Transactable in M"
FROM 
    transactING_merchant_agg
JOIN
    transactABLE_merchant_agg
    USING (M)
WHERE 
    True
```
