# Requirements

- Linux server
  - Allowing inbound SSH and HTTP traffic
  - The following installation assumes Amazon Linux 2023 as OS (based on Fedora)
- JDK 24
- Rust
- PostgreSQL 16

# Installation and Deployment

SSH to your server

### Install rust and cargo
```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Install JDK 24

```bash
cd /opt/

sudo curl -O https://github.com/adoptium/temurin24-binaries/releases/download/jdk-24%2B36/OpenJDK24U-jdk_x64_linux_hotspot_24_36.tar.gz -L

sudo tar xzf OpenJDK24U-jdk_x64_linux_hotspot_24_36.tar.gz

sudo rm OpenJDK24U-jdk_x64_linux_hotspot_24_36.tar.gz

sudo ln -s jdk-24+36 jdk

sudo curl -O https://dlcdn.apache.org/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz

sudo tar xzf apache-maven-3.9.9-bin.tar.gz

sudo rm apache-maven-3.9.9-bin.tar.gz

sudo ln -s apache-maven-3.9.9 mvn
```

Edit `.bash_profile` and append the following
```bash
export PATH=/opt/jdk/bin:/opt/mvn/bin:$PATH
```

Verify Java is working

```bash
source ~/.bash_profile
java -version
```

### Install Postgres 16
```bash
sudo dnf update

sudo dnf install postgresql16-server.x86_64

# change user
sudo su - postgres

postgresql-setup --initdb

# change back to your local user
exit

systemctl enable postgresql.service

systemctl start postgresql.service
```

### Configure Postgres networking

You might have to follow the guide if you see local connection errors. Follow this guide,
https://stackoverflow.com/a/3278835


### PostgreSQL setup

```sql
-- as postgres user (use sudo su - postgres)
CREATE ROLE
  prosearch_user
WITH
  LOGIN
  -- password here
  PASSWORD 'pass';

CREATE DATABASE prosearch;

\c prosearch

GRANT CONNECT ON DATABASE prosearch TO prosearch_user;
GRANT USAGE ON SCHEMA public TO prosearch_user;

-- existing public schema
GRANT ALL ON SCHEMA public TO prosearch_user;

ALTER DEFAULT PRIVILEGES FOR USER prosearch_user
GRANT ALL ON SCHEMAS TO prosearch_user;

ALTER DEFAULT PRIVILEGES FOR USER prosearch_user
IN SCHEMA public
GRANT ALL ON TABLES TO prosearch_user;

ALTER DEFAULT PRIVILEGES FOR USER prosearch_user
IN SCHEMA public
GRANT ALL ON SEQUENCES TO prosearch_user;
```

### Build

Clone prosearch to home folder (You might have to install `git` if it is not available on the server)

For prosearch web app,
```bash

# In the home directory
sudo mkdir /opt/prosearch

sudo chown "$USER:$USER" /opt/prosearch

git clone git@github.com:milindmantri/prosearch.git /opt/prosearch/build

cd /opt/prosearch/build

# prepend MAVEN_OPTS="-Xmx1024m" if on a small server
mvn install -Dmaven.test.skip=true

cp /opt/prosearch/build/target/prosearch-1.0.0-jar-with-dependencies.jar /opt/prosearch/run.jar

cp -r /opt/prosearch/build/tantivy-cli/index-init /opt/prosearch/index

sudo dnf install gcc

# use --jobs=1 if on a small server
cargo install --locked --profile release-speed --path /opt/prosearch/build/tantivy-cli --root /opt/prosearch/tantivy
```

### Services setup

Create the following services,

Prosearch service
`/etc/systemd/system/prosearch.service`
```ini
[Unit]
Description=Prosearch web server and crawler
After=tantivy-index.service postgresql.service
Requires=tantivy-index.service
AssertPathExists=/opt/prosearch/run.jar

[Install]
WantedBy=multi-user.target

[Service]
Type=exec
# to activate an option remove "-X-
ExecStart=java \
            # use: clean crawler data
            -D-X-clean-crawler-data=false \
            # use: set limit of links to crawl per domain
            -D-X-per-host-crawling-limit=10000 \
            # use: how long between each site fetch for the same domain
            -D-X-crawl-download-delay-seconds=1 \
            # use: how long to delay between crawls
            -Ddelay-hours-between-crawls=12 \
            # use: tantivy index server host:port
            -Dtantivy-server=http://localhost:3000 \
            -Dhttp-server-port=80 \
            -Ddb-name=prosearch \
            -Ddb-user=prosearch_user \
            -Ddb-pass=pass \
            # use: set info if want to see it working
            -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
            # max memory
            -Xmx768m \
            # crawler each doc cache
            -Dcrwlr-max-memory-inst=30 \
            # crawler total cache
            -Dcrwlr-max-memory-pool=390 \
            -jar /opt/prosearch/run.jar
Restart=always
```

Tantivy index service
`/etc/systemd/system/tantivy-index.service`
```ini
[Unit]
Description=Tantivy based index and HTTP API
# validate index folder exists
AssertPathExists=/opt/prosearch/index/meta.json
AssertPathExists=/opt/prosearch/index/.managed.json

[Install]
WantedBy=multi-user.target

[Service]
Type=exec
ExecStart=/opt/prosearch/tantivy/bin/tantivy serve \
          --index /opt/prosearch/index \
          -p 3000 \
          --host 127.0.0.1
Restart=always
```

Then, execute,

```sh
sudo systemctl enable prosearch.service
sudo systemctl enable tantivy-index.service

sudo systemctl start prosearch.service
sudo systemctl start tantivy-index.service
```

### Verify

Verify services,
```sh
sudo systemctl status prosearch.service
sudo systemctl status tantivy-index.service
```

Go to the `http://<ip-address/search/` (`http` and the trailing `/` are important) in your browser

```
TODO:
- Set up CI - CD
- Set up https
```
