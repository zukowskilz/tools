package com.pg.streams.repository.paimoin;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import static org.apache.paimon.options.CatalogOptions.*;

public class CreateCatalog {
    public static Catalog createFilesystemCatalog(String catalogName) {
        Path catalog = new Path(catalogName);
        Options options = new Options();
        options.set(WAREHOUSE, catalog.toUri().toString());
        options.set(CASE_SENSITIVE, false);
        options.set(LOCK_ENABLED, true);
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }
}
