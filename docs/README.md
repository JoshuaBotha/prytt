# prytt documentation

prytt runs unmodified AWS Glue 6 job scripts on your machine, on
[Sail](https://github.com/lakehq/sail) instead of a JVM.

## Using prytt

For Glue job authors and the people who test their jobs.

| Page | Covers |
|---|---|
| [Install](user/install.md) | Requirements, adding prytt to a project, what it installs, upgrading, turning it off |
| [Run Glue jobs locally](user/running-jobs.md) | Running scripts, job arguments, S3 paths, catalog tables, Iceberg, strict mode, log output |
| [Test with pytest](user/testing.md) | Fixtures, unit and end-to-end job tests, CI |
| [Configuration reference](user/configuration.md) | Every setting, env var, format name and option |
| [Compatibility and limits](user/compatibility.md) | What's real, passed through, stubbed or unsupported; where local runs differ from Glue |
| [Troubleshooting](user/troubleshooting.md) | Symptoms, causes, fixes |

## How prytt works

For solution architects, reviewers, and anyone deciding whether prytt fits.

| Page | Covers |
|---|---|
| [Solution architecture](architecture/overview.md) | Context, goals, components, runtime flows, data layout, deployment shapes, security, risks |
| [Architecture decisions](adr/) | ADRs: in-process Sail, the `.pth` import hook, catalog tables in SQL, Iceberg `glue_catalog` |
| [Glossary](../GLOSSARY.md) | Mapped table, registered table, temp view |

## Changing prytt

For contributors.

| Page | Covers |
|---|---|
| [Developer guide](developer/guide.md) | Set-up, code map, rules, extension seams, common changes, tests |
| [CONTRIBUTING.md](../CONTRIBUTING.md) | Branches, PR titles, definition of done, releasing |
| [Spike findings](research/spikes.md) | The spike results (S1–S7, S-SQL1) the design rests on |
