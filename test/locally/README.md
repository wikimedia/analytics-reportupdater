You can test your reports locally against the analytics slave databases with
the following procedure. Have in mind that you still need ssh access to the
datastores, testing locally is just a convenience.

1. Clone the reportupdater query repository that you want to test, like:
limn-language-data, limn-multimedia-data, limn-mobile-data, etc. Look at the
`config.yaml` file to get the database host the queries connect to. Possible
values are: s1-analytics-slave.eqiad.wmnet, analytics-store.eqiad.wmnet, etc.

2. Create an ssh tunnel to that host using the script `ssh` in this directory.
Edit the script if necessary to replace the host you want to connect to.

3. Create a file named `my.cnf.research` in this folder. It should look like
this (replacing <password> with the database password of the research user):
```
[client]
user=research
password=<password>
```

4. Modify the database section in the query repository `config.yaml` file
to point to localhost:3307, and to point to the .cnf file you just created.
You can copy the example shown in `config_example.yaml`.

6. Run reportupdater from your machine (using update_reports.py), and point to
the query folder you cloned, for example:

    python ./update_reports.py /path/to/limn-mobile-data/mobile/ /tmp/output -l info

This command will execute queries in the limn-mobile-data repo sending results
to /tmp/output. The flag `-l info` prints more helpful logs. Some queries take
a long time to execute, so consider stopping execution if you have collected
enough output for the sake of your test.
