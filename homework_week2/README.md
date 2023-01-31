## Preperation

1. I use vscode on an ubuntu 22.04.1 computer.

2. Activate conda `zoom` environment that I used when following prefect videos. `conda activate zoom`. It was created with `conda create -n zoom python=3.9`

3. In a separate terminal, activate conda `zoom` environment that I used when following prefect videos: `conda activate zoom`. The start the Prefect Orion server: `prefect orion start`. 

4. Create GCP credential block, copy paste the content of json key which has been made earlier in the Google Cloud UI. Name it `ny-rides-gcpcred-bucket`

5. Create GCS bucket block that points to my ny rides bucket. Name it `ny-rides-bucket-block`. Point Gcp Credentials to `ny-rides-gcpcred-bucket`.

Note: as the questions don't build on each other, I have named the files associated with each question with a corresponding number.

Note 2: I have listed the steps I have taken to find the answer. They are neither exhaustive nor consistent (in their level of detail).

## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

### Steps

1. Copy the `etl_web_to_gcs.py` as `q1_etl_web_to_gcs_green.py`. Fix the datetime conversion to accomodate the different column names in green dataset. Change arguments according to quesiton. Change bucket name to `ny-rides-bucket-block`. Change flow name to `q1_etl_web_to_gcs_green` to distinguish it from `etl_web_to_gcs.py`. See the file for further details (some ).

2. Create folder ./data/green

3. Ran the python script with `python q1_etl_web_to_gcs_green.py` 

### Answer
447770

## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows.

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5pm UTC. What’s the cron schedule for that?

### Steps

1. Copy the `etl_web_to_gcs.py` as `q2_etl_web_to_gcs.py`. Change bucket name to `ny-rides-bucket-block` in case I need to run the file.

2. Deployments can be created using the CLI or using a python script. The scheduling can be configured using the CLI or the UI. I decided for using CLI in both cases since it is a simple deployment I want to create. `prefect deployment build ./q2_etl_web_to_gcs.py:q2_etl_web_to_gcs -n q2 --cron "0 5 1 * *" -a"`. 

3. This creates a yaml-file with which we can apply `prefect deployment apply q2_etl_web_to_gcs-deployment.yaml`. 

4. We can then check if the cron settings are correct in the UI.

![](image.png)

### Answer

`0 5 1 * *` schedules for first of every month at 5am UTC (not pm). If it is indeed pm then `0 17 1 * *` should be used.

## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

### Steps

1. Upload parquet data files for Yellow taxi data for Feb. 2019 and March 2019 into my GCS bucket. This can be done manually or by using one of the existing scripts. I decided to use the `etl_web_to_gcs.py` script I configured while following prefect videos. I ran it twice, each time with a different hard-coded month. For reference, it's included as `q3_etl_web_to_gcs_yellow.py`

2. Copy `etl_gcs_to_bq.py` as `eq3_tl_gcs_to_bq.py`

3. Remove the transform function from the script. Moved `df = pd.read_parquet(path)` into `etl_gcs_to_bq()`

4. Create a table in big query according to instructions in this [video](https://www.youtube.com/watch?v=Cx5jt-V5sgE&t=847s). I named mine trips_data_all.question-3. Also, delete all rows ``DELETE FROM `mythic-plexus-375706.trips_data_all.question-3` WHERE TRUE;``

5. Add my credentials to Google cloud configuration (`write_bq` function)

6. From the example, `parameterized_flow.py`, copy-paste parent flow `etl_parent_flow` (as `q3_etl_parent_flow`) and change `if __name__` to point to `etl_parent_flow`. Configure `q3_etl_web_to_gcs` accordingly.

7. Modified code to make the main flow print the total number of rows processed.

8. Create prefect deployment: `prefect deployment build ./q3_etl_gcs_to_bq.py:q3_etl_parent_flow -n q3` 

9. This creates a yaml file where we can add the default arguments if we want to. Apply it: `prefect deployment apply q3_etl_parent_flow-deployment.yaml`


10. In a separate terminal, activate zoom env, then the default agent `prefect agent start -q default`

11. Run the deployment: prefect deployment `run q3_etl-parent-flow/q3 --params='{"months":[2,3], "year":2019, "color":"yellow"}'`

```
20:27:44.088 | INFO    | Flow run 'sophisticated-loon' - Rows: 14851920
20:27:44.119 | INFO    | Flow run 'sophisticated-loon' - Finished in state Completed('All states completed.')
20:27:44.548 | INFO    | prefect.infrastructure.process - Process 'sophisticated-loon' exited cleanly.
```

### Answer 

14851920

## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

### Steps

1. Code for creating a GitHub storage block is located in `q4_create_git_block.py`

2. Copy `q1_etl_web_to_gcs_green.py` (part of solution to question 1) as `q4_etl_web_to_gcs_green.py`. Changed hard-coded parameters and flow name. Added a check to write_local function so it creates the path if not found. When the deployment is run ut will fetch the repository to a local temp folder and complain if it can't find data/green.

3. Push my code to github. 

4. CLI command to create deployment: `prefect deployment build -n q4 -sb 'github/question-4' --apply homework_week2/q4_etl_web_to_gcs_green.py:q4_etl_web_to_gcs_green`. Notice, since I have the script stored in a subdirectory, I need to be in the same directory as the root of the repository. If not, the file will not be found when creating building the deployment or when running the code.


