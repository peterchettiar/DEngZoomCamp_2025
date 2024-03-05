# Module 1 Homework Submission

## Docker & SQL

### Question 1. Knowing docker tags

We run the `docker run --help` command to access the `--help` documentation for the `docker run` command. When you actually run the command, it gives you an exhaustive list of options, as well as their short descriptions, that potentially could be appended to the `docker run` command. This really depends on what is the outcome you are trying to achieve.

So the question is:

_Which tag has the following text?_

```bash
Automatically remove the container when it exits
```
The answer is `-rm` (see image below). 

![image](https://github.com/peterchettiar/DEngZoomCamp_2024/assets/89821181/9f90423b-f547-4a0f-8b83-814e244c4546)

Using the same example from the video lectures, if we run the following command:

```bash
docker run -it --rm --entrypoint=bash python:3.9
```

What actually happens is that docker runs the python image in its specfied version (downloads the images automatically from docker hub if not found in local repository) in interactive mode (i.e. you would be able to run other commands while the application runs in the background) and then opens `bash` shell as the entrypoint. Once we exit the programe, the container is removed.

### Question 2. Understanding docker first run

we run the same command as before, this time without the `--rm` option as follows:

```bash
docker run -it --entrypoint=bash python:3.9
```

And in the `bash` shell, if we run `pip list`, it would list down all the packages and their respective version. As such, the version for the package _wheel_ is `0.42.0`. See command output below.

![image](https://github.com/peterchettiar/DEngZoomCamp_2024/assets/89821181/f02da5fb-e7eb-443a-8a92-c0efed792bb1)

## Prepare Postgres

To answer Question 3 to 6, we would need to load both the datasets into `postgres` and do the queries directly on `pgadmin`. So the steps for uploading the datasets into `postgres` are as follows:
1. First thing we need to do is to write-up a `docker-compose.yml` file so that we can `docker-compose up` both the `postgres` and `pgadmin` containers - see [docker-compose.yml](https://github.com/peterchettiar/DEngZoomCamp_2024/blob/main/Week_1_basics_n_setup/Week_1_Homework/docker-compose.yaml) (there is a slight modification from the one done during lecture - `volumes` for `pgadmin` was added at the end of the file)
2. Once that is done, we can write an ingestion notebook to test if the data can be downloaded, and the connections with the database is stable
3. Next, we write a comprehensive ingestion python script to be used in our dockerfile
4. Write a `Dockerfile`
5. Now, all we need to do is to `docker build` to build the image from our `Dockerfile`, and then `docker run` the container
