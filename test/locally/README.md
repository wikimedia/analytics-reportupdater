To test your reports locally against the analytics slave databases:

1. Create a file named `my.cnf.research` in this folder. It should look like:
```
[client]
user=research
password=<password>
```

2. Temporarily modify the database section in your `config.yaml` file to
mimic the example shown in `config_example.yaml`.

3. Create an ssh tunnel using the script `ssh`.

4. Run reportupdater normally.
