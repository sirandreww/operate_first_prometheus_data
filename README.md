# Operate First, Prometheus Data

This project is made to house statistics data, as well as the software to fetch such data from Prometheus on some clusters provided as a part of Operate First.

## How to fetch data :

You have to sign up to operate first as a membeer of our group:

1. Go to [operate-first/apps](https://github.com/operate-first/apps) and fork the repository.

2. In the forked repository, go to the file `cluster-scope/base/user.openshift.io/groups/prometheus-ai/group.yaml` and add your GitHub username under users (no capital letters, no extra spaces). It is recommended that you do this step in GitHub (without cloning locally) to avoid unecessary complexities (dos2unix differences and whatnot)

3. In your forked version of the repository you should have a button to open a pull request to the original 'apps' repository. Open a pull request and explain who you are and why are you signing up, here's an example of our [pull request](https://github.com/operate-first/apps/pull/1884)


Notes: 

* Your PR should have only one commit. If you make more than one, squash them or remove the PR and make a new one encompassing all your changes in one commit.

* The PR that created the group in the first place (in case you want to make yor own group) you can find [here](https://github.com/operate-first/apps/pull/1308)

* The PR that added the project inside the group you can find [here](https://github.com/operate-first/apps/pull/1375)

* Furthermore, PR that asked for more storage can be found [here](https://github.com/operate-first/apps/pull/1567)

* An issue to `operate-first/apps` that contained much of how this process was discovered can be found [here](https://github.com/operate-first/support/issues/454)


## How to connect with Operate First members on Slack :


So you want to see ask more questions, Good!

1. Join the Slack workspace [here](https://join.slack.com/t/operatefirst/shared_invite/zt-o2gn4wn8-O39g7sthTAuPCvaCNRnLww). The link can also be found on [operate first's](https://www.operate-first.cloud/) website in the top right corner as of writing this.

2. Join the channel `prometheus-ai`.


## Generating Personal Access Token

To use some of our software you need to aquire a 'personal access token' an official tutorial on how to do that can be found [here](https://www.operate-first.cloud/apps/content/observatorium/thanos/thanos_programmatic_access.html). However, we found it to be unclear so we made our own:

1. Go to [operate first's cosole](https://console-openshift-console.apps.smaug.na.operate-first.cloud/) (this link is specific to smaug).
2. Click on `operate-first` when asked what to login with.
3. Click on your name in the **TOP RIGHT** corner.
4. Then Click on `Copy login command`.
5. Click on `operate-first` when asked what to login with.
6. Click on `Display Token`.
7. Copy what's under `Your API token is`, should look something like this `sha256~nIHsUTlKA2QnDQOmgyzWjaEx5-2xav2e4EsXit-dJFk`

## Repository Containing More Information

Here is a repository that much of this information is taken from:

[Ofir-Shechtman/PrometheusAI](https://github.com/Ofir-Shechtman/PrometheusAI)

## Run Data Fetching code :

Change directory to `src/` and then run `main.py` like so :

```
cd src/
python main.py
```

Install packages when required.

## Script explanation

### Step 1 - main.py

Everytime the script runs,
it begins fetching data starting from the previous hour
backwards. <br>
For example if you ran the script on 09/06/2022 at 12:52
it would start fetching data in these time slots:<br>

1. 09/06/2022 at 11:00 - 09/06/2022 at 12:00
2. 09/06/2022 at 10:00 - 09/06/2022 at 11:00
3. 09/06/2022 at 9:00 - 09/06/2022 at 10:00
4. ...

And so on for about 10 days back.

What data are we pulling?

1. Memory-usage data for each container using this Prometheus query `sum(container_memory_working_set_bytes{
   name!~".*prometheus.*", image!="", container!="POD", cluster="moc/smaug"}) by (container, pod, namespace, node)`.
   Notice that the query is filtering for containers in the "smaug" cluster, that are not a part of Prometheus, that
   have non-empty images, that are not run by the pod itself. Then the query simply groups the containers with the same
   name, pod, namespace and node.
   
2. CPU-usage data for each container using this Prometheus query `sum(rate(container_cpu_usage_seconds_total{name
   !~".*prometheus.*", image!="", container!="POD", cluster="moc/smaug"}[5m])) by (container, pod, namespace, node)`.
   Notice that the query is filtering for containers in the "smaug" cluster, that are not a part of Prometheus, that
   have non-empty images, that are not run by the pod itself. `container_cpu_usage_seconds_total` is a metric that
   counts how many cpu seconds in total a container used, performing rate over that gives us the usage in some time 
   interval. Then the query simply groups the containers with the same
   name, pod, namespace and node.
   
3. Memory-usage percentage data for each node using this Prometheus query `node_memory_Active_bytes/
   node_memory_MemTotal_bytes*100`.
   
### Step 2 - merge_data.py

After having ran the previous step for months to fetch data
from Prometheus, we now have thousands of csv files for each
metric, each file corresponds to some hour in that time 
period. This step is meant to merge all the data into one 
single csv file.

This script takes the data in the format above and merges 
it. The merging process takes into account that some hours
may have been missed when fetching for months, and leaves
csv files that have continuous hours only.

Those csv files may still have missing data in them, and 
require further processing.
   
## Why are we doing this?

The project is part of a project in intelligent system at the "Technion - Israel Institute of Technology".
You can get more information about the project in the project's repository :
[sirandreww/236754_project_in_intelligent_systems](https://github.com/sirandreww/236754_project_in_intelligent_systems.git)

## Link To Data :

Here is a link to where the data can be viewed:

[link to data](https://drive.google.com/drive/folders/1Zpye95sOnMdO6dw0wTuai-s-2BJGUmiP?usp=sharing)
