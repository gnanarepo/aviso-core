
# Local Development Setup Guide

This guide outlines the steps required to set up and run the `aviso-core` project locally.

## 1. Prerequisites

Before starting, ensure you meet the following requirements:

* **VPN Access:** You must be connected to the AWS VPN profile to access the internal MongoDB servers.
* **Bastion Access:** You need SSH access to the bastion server (`bastion.aviso.com`).

## 2. Environment Configuration

1. Locate the `.example.env` file in the project root.
2. Create a copy of this file and name it `.env`.
3. Populate the variables in `.env` with your specific credentials and local configurations.

```bash
cp example.env .env

```

## 3. Database Tunneling (PostgreSQL)

To access the PostgreSQL database securely from your local machine, you must establish an SSH tunnel.

Run the following command in your terminal. Replace `user.name` with your own bastion username.
For example below is an example of url to connect to preprod database.
```bash
ssh -o ExitOnForwardFailure=yes -f -N -L 5502:stagepostgresdb.chm9s1xog441.us-east-1.rds.amazonaws.com:5432 user.name@bastion.aviso.com -p 54863

```

> **⚠️ Important:**
> Ensure that the user in the SSH command above matches the user specified in your PostgreSQL connection string in the `.env` file. If these do not match, you may be unable to access the Postgres database locally.

## 4. Project Installation

Set up a Python virtual environment and install the required dependencies:

1. **Create a virtual environment:**
```bash
python3 -m venv .venv

```


2. **Activate the virtual environment:**
* **Linux/MacOS:**
```bash
source .venv/bin/activate

```


* **Windows:**
```bash
.venv\Scripts\activate

```

First run this command once in the terminal to map the GitHub URLs to use SSH instead of HTTPS:

```bash
git config --global url."ssh://git@github.com/".insteadOf "https://github.com/"
```

Now Install the dependencies:
3. **Install dependencies:**
```bash
pip install -r requirements.txt

```



## 5. Running the Application

Once the VPN is active, the SSH tunnel is established, and dependencies are installed, you can start the Django development server:

```bash
python manage.py runserver

```

You should now be able to successfully hit the APIs at `http://127.0.0.1:8000/`.

---

### Troubleshooting

* **MongoDB Connection Error:** Ensure your VPN is connected.
* **Postgres Connection Error:** Verify that the SSH tunnel command is running in the background and that port `5502` is available locally.
* Sometimes you'll need to rerun the ssh tunnel command if the connection drops. or you get error like is it accepting TCP connections.


### Docker Deployment and Run Locally Via Docker

*** Create Wheel Commands:
--> python3 -m venv .venv 
--> source .venv/bin/activate
--> pip install build 
--> python -m build
--> Check dist folder to check the version of wheel created 

1. EventBus Package Repo :Clone Locally, changed branch and Create Wheel of EventBus, get the version of wheel and add in project.toml of aviso-infra repo, eg: "eventbus==1.0"

2. AvisoSDK Package Repo: Clone Locally, switch to changed branch and Create Wheel of AvisoSDK, get the version of wheel and add in project.toml of aviso-infra repo, eg: "avisosdk==1.0.0"

3. In Aviso-Infra Package Repo:Clone Locally, switch to changed branch and Create Wheel of AvisoInfra, get the version of wheel , add version in req.txt of aviso-core repo.

4. In Wheels Folder of Aviso-Core Project, Copy the newly created wheel files after removing the existing ones.

5. Change ENV: PG_DB_CONNECTION_URL -- replace localhost with host.docker.internal

6. Builder Docker Image :  docker build --no-cache -t aviso-core .

7. Run Docker Locally: docker run --env-file .env -p 8000:8000 aviso-core 