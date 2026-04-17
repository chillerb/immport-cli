# immport-cli

> Simple CLI for the unofficial ImmPort API Python client

## Usage

```sh
# show info
immport-cli about SDY2015 --username ${USERNAME} --password ${PASSWORD}

# get file manifest
immport-cli manifest SDY2015 --username ${USERNAME} --password ${PASSWORD} -o manifest.json

# download result files from a manifest
immport-cli download --manifest manifest.json --username ${USERNAME} --password ${PASSWORD} --results-only

# download files for a study 
immport-cli download --study SDY2015 --username ${USERNAME} --password ${PASSWORD} --results-only
```

