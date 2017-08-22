DROP TABLE IF EXISTS Versions;
DROP TABLE IF EXISTS Editions;
DROP TABLE IF EXISTS Datasets;
DROP TABLE IF EXISTS Contacts;
DROP TABLE IF EXISTS Dimensions;


CREATE TABLE Contacts(
  contactId TEXT PRIMARY KEY,
  name TEXT,
  telephone TEXT,
  email TEXT
);

ALTER TABLE Contacts
  ADD CONSTRAINT filterContacts
  UNIQUE (name, telephone, email);

CREATE TABLE Dimensions(
  instanceId TEXT PRIMARY KEY,
  codeListId TEXT,
  name TEXT,
  type TEXT
);

CREATE TABLE Datasets (
  datasetid TEXT PRIMARY KEY,
  nextRelease TEXT,
  name TEXT,
  contactId TEXT,
  FOREIGN KEY(contactId) REFERENCES Contacts(contactId)
);

CREATE TABLE Editions (
  datasetid TEXT,
  edition TEXT,
  PRIMARY KEY (datasetid, edition),
  FOREIGN KEY(datasetid) REFERENCES Datasets(datasetid)
);

CREATE TABLE Versions (
  datasetid TEXT,
  edition TEXT,
  version TEXT,
  releaseDate TEXT,
  PRIMARY KEY (datasetid, edition, version),
  FOREIGN KEY(datasetid,edition) REFERENCES Editions(datasetid,edition)
);

INSERT INTO public.contacts(
            contactid, name, telephone, email)
    VALUES ('9C0FC5F8-38AA-4C9C-B6B5-2616D9D3666C', 'Name123', '012345-000-000', 'me@test.com');

INSERT INTO public.datasets(
            datasetid, nextrelease, name, contactid)
    VALUES ('88C0F41F-B8EE-4CDF-B582-D6FFF64CDE4B', '0002', 'CPI', '9C0FC5F8-38AA-4C9C-B6B5-2616D9D3666C');

INSERT INTO public.datasets(
            datasetid, nextrelease, name, contactid)
    VALUES ('4E662AB2-A352-46F8-B76C-BF04D2129419', '0003', 'Births and deaths', '9C0FC5F8-38AA-4C9C-B6B5-2616D9D3666C');

INSERT INTO public.editions(
            datasetid, edition)

    VALUES ('88C0F41F-B8EE-4CDF-B582-D6FFF64CDE4B', '2007');
INSERT INTO public.editions(
            datasetid, edition)
    VALUES ('88C0F41F-B8EE-4CDF-B582-D6FFF64CDE4B', '2015');

INSERT INTO public.editions(
            datasetid, edition)
    VALUES ('4E662AB2-A352-46F8-B76C-BF04D2129419', '2016');

INSERT INTO public.versions(
            datasetid, edition, version, releaseDate)
    VALUES ('4E662AB2-A352-46F8-B76C-BF04D2129419', '2016', '1', '00000');

INSERT INTO public.versions(
            datasetid, edition, version, releaseDate)
    VALUES ('88C0F41F-B8EE-4CDF-B582-D6FFF64CDE4B', '2015', '1', '00000');

INSERT INTO public.versions(
            datasetid, edition, version, releaseDate)
    VALUES ('88C0F41F-B8EE-4CDF-B582-D6FFF64CDE4B', '2015', '2', '00000');

INSERT INTO public.versions(
            datasetid, edition, version, releaseDate)
    VALUES ('88C0F41F-B8EE-4CDF-B582-D6FFF64CDE4B', '2007', '1', '00000');




