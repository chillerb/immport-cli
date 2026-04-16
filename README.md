# immport-cli

> Simple CLI for the unofficial ImmPort API Python client

## Usage

```sh
# show info
immport-cli about SDY1 --username ${USERNAME} --password ${PASSWORD}

# get file manifest
immport-cli manifest SDY1 --username ${USERNAME} --password ${PASSWORD} -o manifest.json

# download files
immport-cli download manifest.json --username ${USERNAME} --password ${PASSWORD}
```
