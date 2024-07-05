# M2 CDC Indexer
This module is used for watching redis streams and index provided ids.

Expectations:
1. configuration will be in `app/etc/env.php`
2. process will run as long as memory allows.

TODOs:
1. Report to NewRelic transaction for each message separately
2. Enable concurrent workers

Start as usual:
```bash
bin/magento cdc:watch catalog_category_product
```

For more help:
```
php bin/magento cdc:watch --help
```
