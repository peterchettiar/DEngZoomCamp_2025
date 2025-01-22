# Week 4 Notes

### Table of contents

- [Introduction to Analytics Engineering](#introduction-to-analytics-engineering)
  - [What is Analytics Engineering?](#what-is-analytics-engineering)
  - [Data Modeling Concepts](#data-modeling-concepts)
    - [ETL vs ELT](#etl-vs-elt)
    - [Dimensional Modeling](#dimensional-modeling)
- [Introduction to dbt](#introduction-to-dbt)
  - [What is dbt?](#what-is-dbt)
  - [How does dbt work?](#how-does-dbt-work)
  - [How to use dbt?](#how-to-use-dbt)
- [Setting up dbt](#setting-up-dbt)
  - [dbt Cloud](#dbt-cloud)
  - [dbt Core](#dbt-core)
- [Developing with dbt](#developing-with-dbt)
  - [Anatomy of a dbt model](#anatomy-of-a-dbt-model)
  - [The FROM clause](#the-from-clause)
  - [Defining a source and creating a model](#defining-a-source-and-creating-a-model)
  - [Macros](#macros)
  - [Packages](#packages)
  - [Variables](#variables)
  - [Referencing older models in new models](#referencing-older-models-in-new-models)
- [Testing and documenting dbt models](#testing-and-documenting-dbt-models)
  - [Testing](#testing)
  - [Documentation](#documentation)
- [Deployment of a dbt project](#deployment-of-a-dbt-project)
  - [Deployment basics](#deployment-basics)
  - [Continuous Integration](#continuous-integration)
  - [Deployment using dbt Cloud](#deployment-using-dbt-cloud)
  - [Deployment using dbt Core (local)](#deployment-using-dbt-core-local)
- [Data visualization](#data-visualization)
  - [Google Data Studio](#google-data-studio)
  - [Metabase](#metabase)

# Introduction to Analytics Engineering

## What is Analytics Engineering?

As the _data domain_ has developed over time, new tools have been introduced that have changed the dynamics of working with data:

1. Massively parallel processing (MPP) databases
    * Lower the cost of storage in computing
    * e.g. BigQuery, Snowflake, Redshift
1. Data-pipelines-as-a-service
    * Simplify the ETL process
    * e.g. Fivetran, Stitch
1. SQL-first / Version control systems
    * e.g. Looker
1. Self service analytics
    * e.g. Mode
1. Data governance

The introduction of all of these tools changed the way the data teams work as well as the way that the stakeholders consume the data, creating a gap in the roles of the data team. Traditionally:

* The ***data engineer*** prepares and maintains the infrastructure the data team needs.
* The ***data analyst*** uses data to answer questions and solve problems (they are in charge of _today_).
* The ***data scientist*** predicts the future based on past patterns and covers the what-ifs rather than the day-to-day (they are in charge of _tomorrow_).

However, with the introduction of these tools, both data scientists and analysts find themselves writing more code even though they're not software engineers and writing code isn't their top priority.  Data engineers are good software engineers but they don't have the training in how the data is going to be used by the business users.

The ***analytics engineer*** is the role that tries to fill the gap: it introduces the good software engineering practices to the efforts of data analysts and data scientists. The analytics engineer may be exposed to the following tools:
1. Data Loading (Fivetran, Stitch...)
1. Data Storing (Data Warehouses)
1. Data Modeling (dbt, Dataform...)
1. Data Presentation (BI tools like Looker, Mode, Tableau...)

This lesson focuses on the last 2 parts: Data Modeling and Data Presentation.

## Data Modeling Concepts

### ETL vs ELT

In lesson 2 we covered the difference between [ELT and ETL](https://github.com/peterchettiar/DEngZoomCamp_2025/tree/dbt_cloud/Module-2-workflow-orchestration#etl-vs-elt).

![image](https://github.com/user-attachments/assets/ba6ad322-b207-40a4-a2cd-18cb6fbe1ab9)

Key difference are summarised in a table as follows:

| **Aspect**             | **ETL (Extract, Transform, Load)**               | **ELT (Extract, Load, Transform)**              |
|-------------------------|--------------------------------------------------|------------------------------------------------|
| **Order of Operations** | Extract → Transform → Load                      | Extract → Load → Transform                     |
| **Transformation**      | Performed in an intermediary tool               | Performed within the target system             |
| **Data Handling**       | Suited for structured data                      | Handles structured, semi-structured, and unstructured data |
| **Performance**         | Slower for large datasets due to pre-loading transformations | Faster, leveraging modern cloud platforms      |
| **Tools**               | Legacy tools like Informatica, DataStage, SSIS  | Cloud-native tools like Snowflake, Databricks, dbt |
| **Use Cases**           | Traditional data warehouses, strict pre-load transformations | Modern cloud data warehouses, big data processing |
| **Advantages**          | Consistent transformations, good for smaller datasets | Scalability, speed, and simplified workflows   |
| **Disadvantages**       | Slower and more resource-intensive for large data volumes | Requires powerful target systems for transformations |

In this lesson we will cover the _transform_ step in the ELT process.

### Dimensional Modeling

[Ralph Kimball's Dimensional Modeling](https://www.wikiwand.com/en/Dimensional_modeling#:~:text=Dimensional%20modeling%20(DM)%20is%20part,use%20in%20data%20warehouse%20design.) is an approach to Data Warehouse design which focuses on 2 main points:
* Deliver data which is understandable to the business users.
* Deliver fast query performance.

Other goals such as reducing redundant data (prioritized by other approaches such as [3NF](https://www.wikiwand.com/en/Third_normal_form#:~:text=Third%20normal%20form%20(3NF)%20is,integrity%2C%20and%20simplify%20data%20management.) by [Bill Inmon](https://www.wikiwand.com/en/Bill_Inmon)) are secondary to these goals. Dimensional Modeling also differs from other approaches to Data Warehouse design such as [Data Vaults](https://www.wikiwand.com/en/Data_vault_modeling).

Dimensional Modeling is based around 2 important concepts:
* ***Fact Table***:
    * _Facts_ = _Measures_
    * Typically numeric values which can be aggregated, such as measurements or metrics.
        * Examples: sales, orders, etc.
    * Corresponds to a [_business process_ ](https://www.wikiwand.com/en/Business_process).
    * Can be thought of as _"verbs"_.
* ***Dimension Table***:
    * _Dimension_ = _Context_
    * Groups of hierarchies and descriptors that define the facts.
        * Example: customer, product, etc.
    * Corresponds to a _business entity_.
    * Can be thought of as _"nouns"_.
* Dimensional Modeling is built on a [***star schema***](https://www.wikiwand.com/en/Star_schema) with fact tables surrounded by dimension tables.

A good way to understand the _architecture_ of Dimensional Modeling is by drawing an analogy between dimensional modeling and a restaurant:
* Stage Area:
    * Contains the raw data.
    * Not meant to be exposed to everyone.
    * Similar to the food storage area in a restaurant.
* Processing area:
    * From raw data to data models.
    * Focuses in efficiency and ensuring standards.
    * Similar to the kitchen in a restaurant.
* Presentation area:
    * Final presentation of the data.
    * Exposure to business stakeholder.
    * Similar to the dining room in a restaurant.

# Introduction to dbt

## What is dbt?

***dbt*** stands for ***data build tool***. It's a _transformation_ tool: it allows us to transform process _raw_ data in our Data Warehouse to _transformed_ data which can be later used by Business Intelligence tools and any other data consumers.

A more formal definition would be that dbt is a transformation workflow that allows anyone that knows SQL to deploy analytics code following software engineering best practices like modularity, portability, CI/CD, and documentation.

![image](https://github.com/user-attachments/assets/f50d3375-0e01-4ec0-b80c-2ee5e916fad1)

dbt also allows us to introduce good software engineering practices by defining a _deployment workflow_:
1. Develop models
1. Test and document models
1. Deploy models with _version control_ and _CI/CD_.

![image](https://github.com/user-attachments/assets/b965eabb-e972-4f98-a683-c39657e7acbb)

## How does dbt work?

dbt works by defining a ***modeling layer*** that sits on top of our Data Warehouse. The modeling layer will turn _tables_ into ***models*** which we will then transform into _derived models_, which can be then stored into the Data Warehouse for persistence.

A ***model*** is a .sql file with a `SELECT` statement; no DDL (Data Definition Language - e.g. `CREATE`, `ALTER`, `DROP`, etc.) or DML (Data Manipulation Language - e.g. `SELECT`, `INSERT`, `UPDATE`, etc.) is used. dbt removes all the complexities and generate the DDL and DML for us. And will `dbt compile` the file as well as `dbt run` it in our Data Warehouse. In simpler terms, in the case of the example model from the lecture, dbt will take two raw table from the staging area and transform them to create either a table or a view to the next layer in the data warehouse.

## How to use dbt?

dbt has 2 main ways of using dbt: _dbt Core_ and _dbt Cloud_:
* ***dbt Core***: open-source project that allows the data transformation.
    * Builds and runs a dbt project (.sql and .yaml files).
    * Includes SQL compilation logic, macros and database adapters.
    * Includes a CLI interface to run dbt commands locally.
    * Open-source and free to use.
* ***dbt Cloud***: SaaS (Software as a service) application to develop and manage dbt projects.
    * Web-based IDE to develop, run and test a dbt project.
    * Jobs orchestration.
    * Logging and alerting.
    * Intregrated documentation.
    * Free for individuals (one developer seat).

For integration with BigQuery we will use the dbt Cloud IDE, so a local installation of dbt core isn't required. For developing locally rather than using the Cloud IDE, dbt Core is required. Using dbt with a local Postgres database can be done with dbt Core, which can be installed locally and connected to Postgres and run models through the CLI.

![image](https://github.com/user-attachments/assets/97115ecb-b736-45c9-82a7-4eca83303915)

# Setting up dbt

Before we begin, go to BigQuery and create 2 new empty datasets for your project: a _development_ dataset and a _production_ dataset. Name them any way you'd like.

> Note: Since I'm using BigQuery, I need to use dbt cloud. Hence I will be following the Alternative A video with some minor changes (i.e. steps for setup that actually worked for me since the video did not really cover most of the setup process). Check out [video source](https://www.youtube.com/watch?v=J0XCDyKiU64&list=PLaNLNpjZpzwgneiI-Gl8df8GCsPYp_6Bs&index=4)

## dbt Cloud

1. Create a BigQuery Service account

- In order to connect BigQuery to our DBT project we need to generate a service account JSON file from  [BigQuery credential wizard](https://console.cloud.google.com/apis/credentials/wizard).
- On the landing page you need to fill out the details as follows before clicking on the `NEXT` button:

￼![image](https://github.com/user-attachments/assets/54af621d-f2f6-41ba-974c-1e9958025295)

- Your next page should look something like this:

￼![image](https://github.com/user-attachments/assets/7e374fb0-e1d4-440d-81b7-f869cd247903)

- Fill out the page as follows :

A. Service account name - dbt-service-account

B. Service account ID - **This would be automatically generated based on the service account name - NO NEED TO FILL**

C. Service account description (OPTIONAL) -  Service account for dbt cloud

- After you click on create and continue, you need to add the roles for the service account

![image](https://github.com/user-attachments/assets/f685c4fc-f875-4ad5-a1f2-ef45634a4a89)

Add the following roles:

A. BigQuery Admin

B. BigQuery Data Editor

C. BigQuery Job User

D. BigQuery User

- Once you click on `Done`, you can now proceeds to generate the private key for the service account
- Go to the keys section, select "create new key". Select key type JSON and once you click on create it will get immediately downloaded for you to use. (The JSON key file should be in your downloads folder)

![image](https://github.com/user-attachments/assets/c805591d-55a5-47cc-a522-747b76d82025)

2. Create a dbt cloud project

- Now that we have created a service account for [dbt cloud](https://www.getdbt.com/pricing/), we can proceed to the dbt website to create a project (you can create a free account that entails one free project which suitable enough for our learning)
- Your landing page once you’ve logged in should look something like this:

![image](https://github.com/user-attachments/assets/c506038c-f84a-4b48-abe8-c00e9e23bc1e)

> Note: if you have a default project already created, you can simply click on the project name and click on edit once the project details pop up and press delete in the subsequent landing page

- Click on `New Project` on the top right and this should lead you to the `Set up a new project` page where you are to fill out three things: `Name your project`; `Configure your development environment`; `Setup your repository`
- For the first field `Name your project` you can simply type `taxi_rides_ny` and click continue
- Next for `Configure your development environment`, you would need to add new connection in the connection drop down and it should take you to another page where you have to select biqquery as that is probably what you are using. After giving a connection name (in my case, simply big query), it should look like this:

![image](https://github.com/user-attachments/assets/5c728896-136f-4799-b91a-0491abaad5ce)

- Next click on `Upload a Service account json file` and load the JSON file from the downloads folder (remember this was the key file that we had generated for our service account from GCP)
- It should automatically populate the fields and then click on save at the top right.
- Now that we have created our connection, this should be reflected on the project setup page as well. Simply click on it and test connection (this should be successful), after which you click on save.

![image](https://github.com/user-attachments/assets/c84cf112-9e6a-4274-9fec-6f5f3ef7ca8d)

> Note: There will be a few fields that would be automatically filled at the stage where you have to test connection (Dataset, Target Name and Threads), please note that when you make your first `dbt run` on your cloud IDE, the dataset name should appear in BigQuery which should be indicating that the connection to BigQuery from dbt was successful, and that the dataset is a sandbox dataset for development.

- Set up a repository, for this section click on `Git Clone` and add the git URL to our repo (e.g. git@github.com:peterchettiar/DEngZoomCamp_2025.git)
- Once you’ve added, deploy keys will be generated. Dbt will use the deploy keys to clone the repository, hence in the next section we will discuss more on how to add the deploy keys in your Github account.

3. Connect Github project repo to dbt cloud project

- Go to your project repo and click on settings.

![image](https://github.com/user-attachments/assets/88137b60-6b09-48b1-9216-fd25f404a1fc)

- Next on the settings page, look for `Deploy keys` under security on the left navigation panel

![image](https://github.com/user-attachments/assets/956db92d-821b-4883-a4fc-1ec84a347dad)

- Now you can click on add deploy key and add the title and key value from your dbt project setup page (REMEMBER: check the write access box so that you are able to push your commits).
- Once that is done, I would advise to go back to your profile where you can edit your project details

![image](https://github.com/user-attachments/assets/8760747e-3ec5-4076-b859-e2ab61082c91)

- Click on edit and add the project description as well as the project subdirectory. This will be useful when initialising the project on the cloud IDE first.

4. Initialising dbt project

- This process is essential as it creates the foundational file structure for the project.
- To complete this step now that we have done the basic project setup (i.e. connection BigQuery and Github) we can now proceed to our cloud IDE.
- To do so, we can simply navigate to `Develop` on the left navigation panel and select Cloud IDE and this should set up our environment (which may take a couple of minutes to set up)

![image](https://github.com/user-attachments/assets/8c2a9aa0-f0d9-4d06-b6ab-bff431ba03f9)

- Once in, you should see something like the following:

![image](https://github.com/user-attachments/assets/5fad92db-2c1a-484b-b65e-50481c8b39d0)

- Now click on `Initialize dbt project` and this should create the folders that you need 

5. Configure the dbt Cloud CLI

- Now that we have initialised the dbt project, we can proceed to configure our Cloud CLI so that we can work on the dbt project locally.
- Again on our left navigation panel, click on Develop >> Configure Cloud CLI

![image](https://github.com/user-attachments/assets/f1a07a7f-b4bd-49e2-a385-bb9d34aa0974)

- It should take you to the following page, select Linux on Step 1 since the OS on our VM instance on GCP is Debian GNU/Linux (you can run the command `cat /etc/os-release` to find out your OS of your VM) which is a Linux based system. This should show you the steps for configuration.

![image](https://github.com/user-attachments/assets/bf83b5bb-e78e-4f3a-a1f2-997f567e5b3a)

- For Step 1: Install, go to the dbt Github page to download the latest Linux release (you can run the command `wget https://github.com/dbt-labs/dbt-cli/releases/download/v0.38.23/dbt_0.38.23_linux_amd64.tar.gz` on your VM terminal)
- The reason for choosing that linux release is because my VM is running Debian GNU/Linux 12 (bookworm) with an x86_64 (amd64) architecture.
- Run the following command to unpack the compressed file (lets assume you ran the previous command in your home directory:
```bash
tar -xf dbt_0.29.9_linux_amd64.tar.gz
./dbt --version
```

- Once done, it is advised to create a `.dbt/` hidden folder in your home directory and move the extracted `dbt-cloud-cli` executable to the hidden folder, and create a PATH variable for this executable so as to be able to run dbt program for anywhere (especially useful when you have multiple dbt projects), as follows:
1. `nano ~/.bashrc`
2.  At bottom of page, write:
```bash
export DBT_HOME=~/.dbt/dbt
export PATH=$PATH:$DBT_HOME
```
3. Write out and then run `source ~/.bashrc`

- Run `dbt —version` to verify installation
- For step 2: Configure cloud authentication, we need to download the configuration file to our local downloads folder and then copy it into our instance. My preferred way is to run the following command on our LOCAL terminal (and not the instance command line):
```bash
gcloud compute scp ~/Downloads/dbt_cloud.yml peter@de-zoomcamp:~/.dbt/
```
> Note: I have used the command that I used to highlight the importance of mentioning the user@instance_name as there may be a possibility of having multiple users on the instance. So pick the one where your project repo is in.

- So now for Step 3 : Link your local project to a dbt Cloud project, we need to add the project id provided on the page onto our dbt_projects.yml file as follows:

![image](https://github.com/user-attachments/assets/1c0f2753-a20b-4b6e-b857-1018f5ac3af5)

- Step 4: Invoke a dbt command, now all you have to do is to run the command `dbt compile` to compile your project and validate models and tests (but essentially its just to see if the connection to the dbt cloud project from your local project works).

![image](https://github.com/user-attachments/assets/4135ff92-e6ee-41d5-af11-0a3d8aa3aeb9)

## dbt Core

_[Video source](https://www.youtube.com/watch?v=1HmL63e-vRs&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=37)_

Installing dbt Core locally can be done following the steps in [the official docs](https://docs.getdbt.com/dbt-cli/install/overview). More instructions are also available [in this link](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering/docker_setup).

Starting a dbt project with dbt Core involves creating a `profiles.yml` file manually before running `dbt init`. Check the Video source for more info.
