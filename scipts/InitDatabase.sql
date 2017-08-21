DROP TABLE IF EXISTS Datasets;
DROP TABLE IF EXISTS Contacts;
DROP TABLE IF EXISTS Dimensions;

CREATE TABLE Datasets(
  id TEXT PRIMARY KEY,
  title TEXT,
  url TEXT,
  releaseDate TEXT,
  nextRelease TEXT,
  edition TEXT,
  version TEXT,
  contactId TEXT,
  instanceId TEXT
);

CREATE TABLE Contacts(
  contactId TEXT PRIMARY KEY,
  name TEXT,
  telephone TEXT,
  email TEXT
);

CREATE TABLE Dimensions(
  instanceId TEXT SERIAL PRIMARY KEY,
  codeListId TEXT,
  name TEXT,
  type TEXT,
);

ALTER TABLE Contacts
  ADD CONSTRAINT filterContacts
  UNIQUE (name, telephone, email);
