To successfully build and run the the application using Docker, you need to have the following prerequisites:

# **Install Required Software**

**1. **Install Docker****

Windows & Mac: Download and install [Docker Desktop](https://www.docker.com/pricing/).

**2. **Install Python (>=3.7)****

[//]: # (**3.  Install IntelliJ IDEA**)

[//]: # (   Download and install [IntelliJ IDEA Community or Ultimate]&#40;https://www.jetbrains.com/idea/download/?section=windows&#41;.)

Verify the Docker installation by running 

###### docker --version

# **Run the app**

Start the docker desktop

Open command prompt to run the below command to clone the repo

###### git clone https://github.com/fgyasi/Data-Engineering-Test-E2E.git

Once done, open the terminal to run below commands

###### 

###### docker build -t pyspark_app:latest .

###### docker run --rm docker.io/library/pyspark_app:latest 



