# Module 4 Homework Submission

### Question 1: What happens when we execute dbt build --vars '{'is_test_run':'true'}' You'll need to have completed the "Build the first dbt models" video.

We had only included the conditional statement for our `dev_limit` variable in our staging models, hence the outcome is that only 100 records will be loaded into all the staging bigquery tables.

```sql
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}
```

> Answer: It applies a limit 100 only to our staging models

### Question 2: What is the code that our CI job will run? Where is this code coming from?

We can create `Continuous integration` job in our production environment which runs on pull requests from git.

When a developer creates a pull request (PR) or pushes code to a branch, a CI pipeline automatically runs dbt commands to validate the changes before they are merged into the main branch. It helps catch errors early, ensures data quality, and prevents broken code from being merged into production.

> Answer: The code from the development branch we are requesting to merge to main

### Question 3: What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?

```sql
SELECT
  COUNT(*)
FROM
  `ny-rides-peter-415106.dbt_production.fact_fhvtaxi`
WHERE
  EXTRACT(year
  FROM
    pickup_datetime) = 2019;
```

> Answer: `22998722`

> [!NOTE]
> The link to the model that created the `fact_fhvtaxi` table is [here](https://github.com/peterchettiar/DEngZoomCamp_2025/blob/main/Module-4-analytics-engineering/taxi_rides_ny/models/core/fact_fhvtaxi.sql)

### Question 4: What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table and the fact_trips tile as seen in the videos?
