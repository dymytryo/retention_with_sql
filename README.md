# North Star Metric: Merchant Transaction Volume Retention Rate 

<h2>Description</h2>
The North Star Metric (NSM) is a concept in business strategy that refers to a single metric that captures the core value that a product or service delivers to its customers. It is the key performance indicator (KPI) that a company focuses on in order to measure its success and achieve its long-term goals.
In the given project, I am trying to work through the complexity of measuring success of transitioning our subscribers from traditional payment methods like check into a single use account (SUA). 
<br />

<h2>Languages and Utilities Used</h2>

- <b>AWS Athena</b> using <b>Presto SQL</b> - querying 
- <b>dbt</b> using <b>PostgreSQL</b> - modeling 
- <AWS Quickisght> - visualizing 
- <Lucid> - charting 

<h2>Project walk-through:</h2>

<h3>Finding totals for joined and churned merchants</h3>

To begin, we would want to know the total number of merchants that are entering and leaving the program in the given month. Additionally, we want to calculate what is the total number of merchants gettting paid using SUA in the beginning and the end of each month. 

We have a change log that is recording all the changes for all the fields in the datalake:
* it is cumbersome because of the amount of data -> need to partition and add as many filters as possible; 
* it is missing the information that was done through a manual ETL directly into the database -> add UNIONS to mock those records;

Sample records from the changeLog table (truncated): 
| changeDate  | payerId | recordType |entityId | valCurr | valPrev |  
| ------------------------------------|---| -----------------| --------------- |------------| ------------- |
| timestamp '2012-10-31 01:00:00.000' | str 'payment_method' | str 'rid04XXXXXXXXXX'  | str 'eid04XXXXXXXXXX' | 12 | 6
| timestamp '2012-10-31 01:00:00.000' | str 'subscription_type' | str 'rid01XXXXXXXXXX'  | str 'eid01XXXXXXXXXX' | 12 |11


The query that we are going to use: 
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


This will give us the following output: 
<table style="text-align:center; width:30%; text-align:center;font-size: 100% ">
  <caption style = "font-size: 100%">SQL Output</caption>
  <tr>
    <th>Month</th>
    <th>January 2023</th>
    <th>February 2023</th>
    <th>March 2023</th>
  </tr>
  <tr>
    <td>BOM</td>
    <td>1,000,584</td>
    <td>999,470</td>
    <td>1,015,291</td>
  </tr>
  <tr>
    <td>Joined</td>
    <td>12,584</td>
    <td>25,689</td>
    <td>10,256</td>
  </tr>
  <tr>
    <td>Churned</td>
    <td>13,698</td>
    <td>9,868</td>
    <td>6,987</td>
  </tr>
   <tr>
    <td>EOM</td>
    <td>999,470</td>
    <td>1,015,291</td>
    <td>1,018,560</td>
  </tr>
</table>

From here, we can vizualise the changes to be communicated to the stakeholders: 
![alt text](https://github.com/dymytryo/githubTest/blob/b95e10d6b101626e6a550a7825bacfe5b4f00848/churned_joined_merchants.png?raw=true)

In January and February we see an intake in the number of joined merchants to the program that was catalyzed by the successful outreach campaign. Nevertheless, the next month, we are seeing a huge drop mostly cause by those new merchants. Turns out, the outreach had flaws and changing the payment method in the system for some of these merchant was wrongful.

This is great, but how do we tie this to the revenue, cashflows, and overall initiative performance? We would want:
* to see the changes by cohorts in a given month, so these random exegenous shocks would not affect our forecast;
* to see the impact that it created on the bottom line with revenue;
    
<h3>Modeling with dbt to overcome data volume contstraints</h3>
    
Ideally, we would want to measure the retention rates using a 90-day window period. Usually, this would not require a heavy lift for a one-time analysis, but we would like to have this displayed in the dashboard that will refresh monthly and would show the data for at least one year. 

**Challenge**: serverless query engine - Amazon Athena - has a limitation on the data scanned of 100GB, which limits us usually to pulling data only for one period of a time. 
**Solution**: dbt-tize the model and create marts-level tables with aggregated metrics to do the further analysis:
![alt text](https://github.com/dymytryo/githubTest/blob/d77cc97247fc3c8392b99d3ebbd025081ff05d25/dbt_modeling_flow.png?raw=true)

As a result, we are able to get just the results of the query that we used before. Here, the table with retention data will be a full-fledged version of the CTE that we used before `join_churn_pairs`. 

Now we have the retention detail stored and we can query it directly without restrictions and worrying about the underlying built of previous CTE. This removes the dependency of more skilled analysts and can be distributed across the team for further analyses. 

Result of dbt-izing: 

<table style="text-align:center; width:30%; text-align:center;font-size: 100% ">
  <caption style = "font-size: 100%">SQL Output: **payment_detail_agg**</caption>
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
  <caption style = "font-size: 100%">SQL Output: **merchant_retention_record**</caption>
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

Using the given tables, we would wrap up cohorts and their respective TPV in arrays and maps and then perform the operations on those to get the rates of change and actual dollar value generated. 

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

This will give us the following output: 
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

Next, vizualize the results to communicate findings to stakeholders: 
![alt text](https://github.com/dymytryo/githubTest/blob/4f54f857318544073df4a20d544234c54de339c4/retention_rates.png?raw=true)
Here, based on a 3-month retention, we can see that we have a much higher retention for those merchants who are not getting paid frequently. They do not have to pay interchange rates and hence are not concerned with taking SUA as a payment method. 
On the other hand, those merchants that received the payment three months ago and receive one today, have a higher rate of default. This is associated with additional costs associated with processing SUA, reconsiliation issues, etc. 

![alt text](https://github.com/dymytryo/githubTest/blob/4f54f857318544073df4a20d544234c54de339c4/transaction_volume_merchants.png?raw=true)
Here we can see that the overall volume generated is not going down even though we know that our retention rates are not necessarily that high in the long run. The organge line shows the transactable merchants and the blue one shows the transacting vendors. This confirms that our acquisition efforts do increate the number of new vendors to generate more volume and reach the goals.
