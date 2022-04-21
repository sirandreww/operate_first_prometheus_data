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

Change directory to `src/` and then run `src/main.py` like so :

```
cd src/
python main.py
```

Install packages when required.
