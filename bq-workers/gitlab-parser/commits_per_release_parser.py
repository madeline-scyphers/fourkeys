import json
import re

from google.cloud import bigquery


def process_commits_per_release_event(event, enriched_event):
    # remove mock so it is normalized to github or gitlab
    source = event["source"].strip("mock")
    event_type = event["event_type"]
    metadata = json.loads(event["metadata"])
    deployment_sha = SHA_PARSERS[source][event_type](metadata)

    client = bigquery.Client()
    query = """
    SELECT raw.id, 
           dply.main_commit,
           JSON_EXTRACT_SCALAR(raw.metadata, '$.before') before,
           four_keys.json2array(JSON_EXTRACT(raw.metadata, '$.commits')) array_commits,
           dply.time_created
    FROM `four_keys.events_raw` raw
    LEFT JOIN `four_keys.deployments` dply
        ON raw.id = dply.main_commit
    WHERE raw.event_type = 'push'
    """

    query_job: bigquery.job.QueryJob = client.query(query)
    df = query_job.to_dataframe()

    df["parent_is_release"] = df["before"].isin(df["main_commit"])
    # because of left join, only place where main commit is, is where id == main commit
    # or where main_commit is not nan
    df["is_release"] = ~df["main_commit"].isna()

    df["array_commits"] = df["array_commits"].map(json.loads)
    df["array_commits"] = df["array_commits"].map(lambda ls: [d["id"] for d in ls])

    def recurse(row):
        if row.empty:
            return []
        if row.parent_is_release:
            return row.array_commits
        else:
            new_row = df.loc[df.id == row.before].squeeze()
            if not new_row.empty:
                cmts = recurse(new_row)
            else:
                cmts = [row.before]

            row.array_commits.extend(cmts)
            return row.array_commits

    df_rel = df.loc[df.is_release].copy()
    df_rel = df_rel.sort_values(by="time_created").reset_index(drop=True)
    df_rel["assoc_cmts"] = df_rel.apply(recurse, axis=1)
    df_rel = df_rel.drop(columns=["array_commits"])
    df_rel = df_rel.explode("assoc_cmts")
    df_rel = df_rel.drop_duplicates()
    associated_changes = df_rel.loc[df_rel.main_commit == deployment_sha, "assoc_cmts"].to_list()

    enriched_event["enriched_metadata"]["associated_changes"] = associated_changes
    return enriched_event


def gl_deployment_sha(metadata):
    commit_url = metadata["commit_url"]
    pattern = r".*commit\/(.*)"
    return re.findall(pattern, commit_url)[0]


def gl_pipeline_sha(metadata):
    return metadata["commit"]["id"]


def gh_deployment_sha(metadata):
    return metadata["deployment"]["sha"]


GITLAB_SHA_PARSERS = {
    "deployment": gl_deployment_sha,
    "pipeline": gl_pipeline_sha
}

GITHUB_SHA_PARSERS = {
    "deployment_status": gh_deployment_sha
}

SHA_PARSERS = {
    "gitlab": GITLAB_SHA_PARSERS,
    "github": GITHUB_SHA_PARSERS
}
