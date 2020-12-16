# General Boss Python3 library used by many of the VMs running Boss code.

## activities
Code run on step function activity servers.

## bossutils
General Boss Python3 library used by many of the VMs running Boss code.

### aws
Helps the developer dynamically create AWS credentials and create a boto3 sesssion.

### configuration
Helps with reading data from the boss config file (/etc/boss/boss.config).

### utils
Generic utilities used in multiple locations.

### vault
Helps the developer connect to the target Vault server with the given url and access token.

## cachemgr
Code that runs on the cache manager server.

## lambda
Defines most lambda functions used by Boss.

## lambdafcns
Contains symbolic links to the files in the lambda folder for easier importing
for tests.

## lmbdtest
Unit tests for the lambda functions.

### Running Tests

#### Required Symlink
The lambda functions require Python files contained in `boss-manage.git/lib`.
Make these available by creating a symlink to that folder on your machine and
name it `bossnames`.

```shell
# From the root of the boss-tools repo:
ln -s ~/Documents/MICrONS/boss-manage/lib bossnames
```

#### Boss Dependencies

These Boss repos need to be either installed or in your `PYTHONPATH`
environment variable:

* heaviside
* ndingest
* spdb

```shell
# Example using PYTHONPATH when all repos are sub-folders of ~/Documents/MICrONS:
export PYTHONPATH=~/Documents/MICrONS:~/Documents/MICrONS/spdb:~/Documents/MICrONS/boss-manage/lib/heaviside.git
```

#### Running

Run tests from the root of the repo, but run each sub-folder separately.

```shell
python -m unittest discover activities
python -m unittest discover bossutils
python -m unittest discover cachemgr
python -m unittest discover lmbdtest
```

## Legal

Use or redistribution of the Boss system in source and/or binary forms, with or without modification, are permitted provided that the following conditions are met:
 
1. Redistributions of source code or binary forms must both retain any copyright notices and adhere to licenses for any and all 3rd party software (e.g. Apache).
2. End-user documentation or notices, whether included as part of a redistribution or disseminated as part of a legal or scientific disclosure (e.g. publication) or advertisement, must include the following acknowledgement:  The Boss software system was designed and developed by the Johns Hopkins University Applied Physics Laboratory (JHU/APL). 
3. The names "The Boss", "JHU/APL", "Johns Hopkins University", "Applied Physics Laboratory", "MICrONS", or "IARPA" must not be used to endorse or promote products derived from this software without prior written permission. For written permission, please contact BossAdmin@jhuapl.edu.
4. This source code and library is distributed in the hope that it will be useful, but is provided without any warranty of any kind.

