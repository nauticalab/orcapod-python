# Tutorial

This tutorial teaches Orcapod by building one small analysis from start to finish. It is
written for scientists who are comfortable with Python and have not used Orcapod before. You
can work through it on your own, at your own pace, on a laptop.

Each part takes about 20 minutes: a few minutes of reading, samples to run, and two or three
exercises. Every exercise has a solution you can unfold. Every sample on these pages is run by
Orcapod's test suite, so what a page shows is what the library prints.

## What you will build

A pipeline that takes a small extracellular-recording dataset and computes, for every
recording session, the mean firing rate of its units. Along the way you will store the
results in a database, re-run the pipeline so that only new work is computed, read and write
files, and trace a result back to the rows it came from.

## The dataset

The data is synthetic. A short script generates it on your machine; nothing is downloaded and
no real recordings are involved. It imitates a tiny study:

- 4 mice, two wildtype and two knockout
- 2 recording sessions per mouse
- 2 probes per session (`imec0` in V1, `imec1` in CA1)
- 4 sorted units per probe

```
workshop_data/
    subjects.csv       one row per mouse:    subject_id, sex, age_weeks, genotype
    sessions.csv       one row per session:  subject_id, session, date, duration_s
    units.csv          one row per unit:     subject_id, session, probe, unit,
                                             region, depth_um, n_spikes
    sessions/
        mouse_01_s1/
            notes.txt
            spikes_imec0.csv      one row per spike: time_s, unit
            spikes_imec1.csv
        mouse_01_s2/
        ...
```

27 files, about 0.4 MB. [Part 0](00-setup.md) shows how to generate it.

## The parts

| Part | You learn | Time |
|---|---|---|
| [0. Setup](00-setup.md) | Install Orcapod, check it works, generate the dataset | 10 min |
| [1. Your first stream](01-first-stream.md) | Sources, tag and data columns, looking at a stream | 20 min |
| [2. Computing with function pods](02-function-pods.md) | Turning a Python function into a pipeline step | 20 min |
| [3. Reshaping with operators](03-operators.md) | Joining, filtering, renaming | 20 min |
| [4. Many rows into one](04-many-rows-into-one.md) | Grouping rows and computing over the group | 20 min |
| [5. Keeping results](05-pipeline-job.md) | `PipelineJob`: run a pipeline against a database | 20 min |
| [6. Re-runs and caching](06-reruns-and-caching.md) | What is recomputed, and what is not | 20 min |
| [7. Working with files](07-files.md) | Functions that read and write files | 20 min |
| [8. Inspecting and tracing](08-inspecting-and-tracing.md) | Where a result came from | 20 min |
| [9. When things go wrong](09-when-things-go-wrong.md) | Reading errors; failures inside a run | 15 min |
| [10. Where next](10-where-next.md) | Features this tutorial did not cover | optional |

Parts 0 to 6 are the core and should be done in order. Parts 7 to 10 can be read in any
order afterwards.

## How to use these pages

Type or paste each sample into a Python session, a notebook, or a script, in the order shown.
Within one page, later samples use names defined by earlier ones. Each page starts from
scratch, so you can stop after any part and pick up at the next one later.

Lines starting with `#` directly under a `print(...)` show what it prints. Where a value
differs from run to run (a hash, a timestamp), the page shows `...` in its place.

If something fails, look at [Troubleshooting](../troubleshooting.md) first.
