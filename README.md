To successfully build and run the the application using Docker, you need to have the following prerequisites:

# **Install Required Software**

**1. **Install Docker****

Windows & Mac: Download and install [Docker Desktop](https://www.docker.com/pricing/).



Verify the Docker installation by running 

###### docker --version

# **Run the app**

Start the docker desktop

Open command prompt to run the below command to clone the repo

###### git clone https://github.com/fgyasi/Data-Engineering-Test-E2E.git

Once done, open the terminal to run below commands

###### cd Data-Engineering-Test-E2E

###### docker build -t pyspark_app:latest .

###### docker run --rm docker.io/library/pyspark_app:latest 



