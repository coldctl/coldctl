-- Imported archives retain their original plan/manifest, with a separate local locator.
ALTER TABLE jobs ADD COLUMN imported_root TEXT;
