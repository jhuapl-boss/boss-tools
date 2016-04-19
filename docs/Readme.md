# Generating HTML Documentation

`$BOSSTOOLS` is the location of the boss-tools repository.

```shell
cd $BOSSTOOLS/docs/SphinxDocs

# Ensure Sphinx and the ReadTheDocs theme is available.
pip3 install -r requirements.txt

./makedocs.sh
```

Documentation will be placed in `$BOSSTOOLS/docs/SphinxDocs/_build/html`.
